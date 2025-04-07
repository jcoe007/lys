
from flask import Flask, request, jsonify
from notion_client import Client
import requests
import os
from openai import OpenAI

app = Flask(__name__)

def chunk_text(text, chunk_size=1900):
    return [text[i:i+chunk_size] for i in range(0, len(text), chunk_size)]

@app.route("/notion-webhook", methods=["POST"])
def run_assistant():
    notion = Client(auth=os.environ["NOTION_API_KEY"])
    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

    # Step 1: Query Notion for rows with Status = Ready
    results = notion.databases.query(
        database_id="1cb596796eb480e69b76e1c9da8aa7c6",
        filter={"property": "Status", "select": {"equals": "Ready"}}
    )

    for page in results["results"]:
        props = page["properties"]
        title = props["Name"]["title"][0]["text"]["content"]
        files = props["Document"]["files"]

        if not files:
            continue

        file_obj = files[0]
        file_url = file_obj["file"]["url"]

        try:
            response = requests.get(file_url)
            with open("temp.pdf", "wb") as f:
                f.write(response.content)

            # Assistant prompt
            prompt = (
                "Your task is to extract obligations that the document places on Lightyears. "
                "For each obligation, return the following fields in a readable, plain text format. "
                "Do not use tables or markdown. Use a consistent structure with one obligation per block. "
                "Format each obligation like this:\n\n"
                "🔹 Clause: [Clause reference]\n"
                "• Type: [Type: Trivial, Consult on Event, Scheduled, Conditional]\n"
                "• Trigger: [Trigger if any]\n"
                "• Action: [Action we must take]\n"
                "• Frequency: [How often]\n"
                "• Notes: [Additional notes]\n"
                "• Status: [Captured, Needs review, Unclear]\n\n"
                "Please return each obligation using this format, separated by two new lines."
            )

            uploaded_file = client.files.create(file=open("temp.pdf", "rb"), purpose="assistants")
            response = client.responses.create(
                model="gpt-4o",
                input=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "input_file", "file_id": uploaded_file.id},
                            {"type": "input_text", "text": prompt}
                        ]
                    }
                ]
            )

            output = response.output_text.strip()
            chunks = chunk_text(output)

            # Write result to Notion (Raw result + Status)
            notion.pages.update(
                page_id=page["id"],
                properties={
                    "Status": {"select": {"name": "Complete"}},
                    "Raw result": {
                        "rich_text": [{"type": "text", "text": {"content": chunk}} for chunk in chunks]
                    }
                }
            )

            # Append readable output to the Notion page
            paragraph_blocks = [
                {
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [{
                            "type": "text",
                            "text": {"content": chunk}
                        }]
                    }
                }
                for chunk in chunks
            ]

            notion.blocks.children.append(
                block_id=page["id"],
                children=[
                    {
                        "object": "block",
                        "type": "heading_2",
                        "heading_2": {
                            "rich_text": [{
                                "type": "text",
                                "text": {"content": "📋 Assistant Output"}
                            }]
                        }
                    }
                ] + paragraph_blocks
            )

        except Exception as e:
            import traceback
            print(f"❌ Failed to process page '{title}':", e)
            traceback.print_exc()
            notion.pages.update(
                page_id=page["id"],
                properties={
                    "Status": {"select": {"name": "Failed"}}
                }
            )

    return jsonify({"status": "complete"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
