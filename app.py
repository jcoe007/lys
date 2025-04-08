
from flask import Flask, request, jsonify
from notion_client import Client
import requests
import os
from openai import OpenAI
from threading import Thread

app = Flask(__name__)

def chunk_text(text, chunk_size=1900):
    return [text[i:i+chunk_size] for i in range(0, len(text), chunk_size)]

DEFAULT_PROMPT = (
    "Your task is to extract obligations that the document places on parties, particularly Lightyears. Our goal is to then actively track our obligations, creating specific tasks as needed. For each obligation, return the following fields in a readable, plain text format. Do not use tables or markdown. Use a consistent structure with one obligation per block. Format each obligation in a list like this:"
    "Clause: [Clause reference]"
    "Party: [Which party this obligation falls on]"
    "Trigger: [Trigger event, if Conditional]"
    "Timing: [When the action should take place, if Scheduled]"
    "Frequency: [How often, if Scheduled]"
    "Action: [Action that the party must take, in detail]"
    "Notes: [Additional notes if the above does not fully capture the obligation. Mention here if you feel anything in the obligation is unclear or warrants human comprehension.]"
    "Please return each obligation using this format, separated by two new lines."
)

@app.route("/notion-webhook", methods=["POST"])
def run_assistant():
    Thread(target=process_ready_rows).start()
    return jsonify({"status": "queued"}), 200

def process_ready_rows():
    notion = Client(auth=os.environ["NOTION_API_KEY"])
    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

    results = notion.databases.query(
        database_id="1cb596796eb480e69b76e1c9da8aa7c6",
        filter={"property": "Status", "select": {"equals": "Ready"}}
    )

    for page in results["results"]:
        page_id = page["id"]
        props = page["properties"]
        title = props["Name"]["title"][0]["text"]["content"]
        files = props["Document"]["files"]

        if not files:
            continue

        try:
            notion.pages.update(
                page_id=page_id,
                properties={"Status": {"select": {"name": "Running"}}}
            )
        except Exception as e:
            print(f"❌ Could not mark '{title}' as Running:", e)
            continue

        file_obj = files[0]
        file_url = file_obj["file"]["url"]

        try:
            response = requests.get(file_url)
            with open("temp.pdf", "wb") as f:
                f.write(response.content)

            # Use custom prompt if provided, otherwise fallback to default
            if props.get("Instructions") and props["Instructions"].get("rich_text"):
                prompt = props["Instructions"]["rich_text"][0]["text"]["content"]
            else:
                prompt = DEFAULT_PROMPT

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

            notion.pages.update(
                page_id=page_id,
                properties={
                    "Status": {"select": {"name": "Complete"}}
                }
            )

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
                block_id=page_id,
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
                page_id=page_id,
                properties={"Status": {"select": {"name": "Failed"}}}
            )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
