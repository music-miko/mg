import asyncio
import os

from pymongo import AsyncMongoClient
from pymongo.errors import DuplicateKeyError

# Mongo connection (same URI for both old and new databases)
MONGO_URI = os.getenv("MONGO_URI")

# Old Database
OLD_DB_NAME = os.getenv("OLD_DB_NAME", "Anon")

# New Database
NEW_DB_NAME = os.getenv("NEW_DB_NAME", "MusicBot")


def extract_id(doc: dict, *keys: str):
    """Return the first present, non-null value among `keys`, falling back to
    the document's own _id if it looks like a usable chat/user id (an int, or
    a numeric string). Returns None if nothing usable is found."""
    for key in keys:
        if key in doc and doc[key] is not None:
            return doc[key]

    raw_id = doc.get("_id")
    if isinstance(raw_id, int):
        return raw_id
    if isinstance(raw_id, str) and raw_id.lstrip("-").isdigit():
        return raw_id

    return None


async def migrate_data():
    if not MONGO_URI:
        raise RuntimeError("MONGO_URI environment variable is not set")

    client = AsyncMongoClient(MONGO_URI)
    await client.aconnect()

    old_db = client[OLD_DB_NAME]
    new_db = client[NEW_DB_NAME]

    old_chats = old_db["chats"]
    old_users = old_db["users"]
    new_chats = new_db["chats"]
    new_users = new_db["users"]

    existing_chats = {int(doc["_id"]) async for doc in new_chats.find({}, {"_id": 1})}
    existing_users = {int(doc["_id"]) async for doc in new_users.find({}, {"_id": 1})}

    # Migrate Chats
    chat_migrated = 0
    chat_skipped = 0
    async for chat in old_chats.find({}):
        raw_id = extract_id(chat, "chat_id", "chatId", "id")
        if raw_id is None:
            chat_skipped += 1
            print(f"Skipping chat doc with no usable id: {chat}")
            continue

        try:
            chat_id = int(raw_id)
        except (TypeError, ValueError):
            chat_skipped += 1
            print(f"Skipping chat doc with non-numeric id ({raw_id!r}): {chat}")
            continue

        if chat_id not in existing_chats:
            try:
                await new_chats.insert_one({"_id": chat_id})
                chat_migrated += 1
            except DuplicateKeyError:
                continue
    print(f"Migrated {chat_migrated} new chats. Skipped {chat_skipped} bad docs.")

    # Migrate Users
    user_migrated = 0
    user_skipped = 0
    async for user in old_users.find({}):
        raw_id = extract_id(user, "user_id", "userId", "id")
        if raw_id is None:
            user_skipped += 1
            print(f"Skipping user doc with no usable id: {user}")
            continue

        try:
            user_id = int(raw_id)
        except (TypeError, ValueError):
            user_skipped += 1
            print(f"Skipping user doc with non-numeric id ({raw_id!r}): {user}")
            continue

        if user_id not in existing_users:
            try:
                await new_users.insert_one({"_id": user_id})
                user_migrated += 1
            except DuplicateKeyError:
                continue
    print(f"Migrated {user_migrated} new users. Skipped {user_skipped} bad docs.")

    print("Migration completed successfully!")
    await client.close()


if __name__ == "__main__":
    asyncio.run(migrate_data())
