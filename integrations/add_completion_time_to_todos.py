import json
import time

import requests
from absl import app, flags

flags.DEFINE_string("database_id", None, "The id of the todo database.", required=True)
flags.DEFINE_list(
    "complete_selects",
    "Completed,Archived",
    "The status of the objects to mark as completed.",
)
flags.DEFINE_string("datetime_field", "Completed On", "The datetime to use to mark the todo.")
flags.DEFINE_string("token", None, "The API token of the notion integration.", required=True)
flags.DEFINE_string("status_column", "Status", "The column to use for completion status selection")
flags.DEFINE_integer("sleep_for", 1, "The number of seconds to sleep between requests.")

FLAGS = flags.FLAGS


def main(*unused_argv):

    # Setup the authorization headers
    headers = {
        "Authorization": f"Bearer {FLAGS.token}",
        "Notion-Version": "2021-05-13",
    }

    # Setup the database
    # Get the database, and see if the edit time is in the future.
    res = requests.get(f"https://api.notion.com/v1/databases/{FLAGS.database_id}", headers=headers)
    if res.status_code == 404:
        print(f"Error finding database: {FLAGS.database_id}. Are you sure you shared the DB with the integration?")
        return
    db = res.json()
    if FLAGS.datetime_field not in db["properties"] or db["properties"][FLAGS.datetime_field]["type"] != "date":
        print(f"Database does not have the required field {FLAGS.datetime_field}, or it is not a datetime")
    if (
        FLAGS.status_column not in db["properties"]
        or db["properties"][FLAGS.status_column]["type"] != "select"
        or any(
            f not in [q["name"] for q in db["properties"][FLAGS.status_column]["select"]["options"]]
            for f in FLAGS.complete_selects
        )
    ):
        print(db["properties"][FLAGS.status_column]["select"]["options"])
        print(f"Database does not have a {FLAGS.status_column} column with fields {FLAGS.complete_selects}")
        return

    while True:

        # Query the database for all todos with the status in the complete_selects list
        # and does not have a completion time.
        filter_data = {
            "and": [
                {"property": FLAGS.datetime_field, "date": {"is_empty": True}},
                {"or": [{"property": FLAGS.status_column, "select": {"equals": q}} for q in FLAGS.complete_selects]},
            ]
        }

        objects_needing_updates = []
        while True:
            start_cursor = None
            if start_cursor:
                page = requests.post(
                    f"https://api.notion.com/v1/databases/{FLAGS.database_id}/query",
                    json={"filter": filter_data, "start_cursor": start_cursor},
                    headers=headers,
                )
            else:
                page = requests.post(
                    f"https://api.notion.com/v1/databases/{FLAGS.database_id}/query",
                    json={"filter": filter_data},
                    headers=headers,
                )
            if page.status_code != 200:
                print(f"Error querying database: {FLAGS.database_id}")
                return
            if res.status_code == 429:
                # Back-off, and retry
                time.sleep(FLAGS.sleep_for)
                continue
            page = page.json()
            for q in page["results"]:
                objects_needing_updates.append(q)
            if page["has_more"]:
                start_cursor = page["next_cursor"]
            else:
                break

        for page in objects_needing_updates:
            page_id = page["id"]
            patch = {
                "properties": {
                    FLAGS.datetime_field: {
                        "date": {"start": page["last_edited_time"]},
                    }
                }
            }
            print(f"Updating TODO, id: {page_id}, patch: {patch}")
            res = requests.patch(f"https://api.notion.com/v1/pages/{page_id}", json=patch, headers=headers)

        time.sleep(0.2)


if __name__ == "__main__":
    app.run(main)
