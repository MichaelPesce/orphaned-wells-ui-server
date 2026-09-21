"""Preview schema role changes; run with --apply to persist them."""

import argparse
import json
import time

from ogrre.internal.schema_validation import DESTRUCTIVE_PERMISSION


def migrate_schema_permissions(db, apply=False):
    changes = []
    for role in db.roles.find({}):
        before = role.get("permissions") or []
        permissions = set(before)
        if role.get("category") == "system" or (
            role.get("category") == "team" and role.get("id") == "team_lead"
        ):
            permissions.add("manage_schema")
        if role.get("category") == "system" and role.get("id") == "sys_admin":
            permissions.add(DESTRUCTIVE_PERMISSION)
        else:
            permissions.discard(DESTRUCTIVE_PERMISSION)
        if permissions == set(before):
            continue
        after = sorted(permissions)
        changes.append(
            {
                "id": role.get("id"),
                "category": role.get("category"),
                "before": before,
                "after": after,
            }
        )
        if apply:
            query = {
                "_id": role["_id"],
                "permissions": role["permissions"]
                if "permissions" in role
                else {"$exists": False},
            }
            if not db.roles.update_one(
                query, {"$set": {"permissions": after}}
            ).matched_count:
                raise RuntimeError(
                    "A role changed during migration; preview and retry."
                )
            db.history.insert_one(
                {
                    "action": "migrateSchemaPermissions",
                    "timestamp": time.time(),
                    "query": {
                        "role_id": role.get("id"),
                        "category": role.get("category"),
                        "permissions": after,
                    },
                    "previous_state": {"permissions": before},
                    "notes": "Applied by the schema-permission migration CLI.",
                }
            )
    return changes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    from dotenv import load_dotenv

    load_dotenv()
    from ogrre.internal.mongodb_connection import connectToDatabase

    print(
        json.dumps(
            migrate_schema_permissions(connectToDatabase(), args.apply), indent=2
        )
    )


if __name__ == "__main__":
    main()
