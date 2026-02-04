import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timedelta, timezone

try:
    import pandas as pd
except ImportError:
    pd = None


TENANT_ID = "vWxkIvCXIJUSIlhBSiIl"


def init_firebase() -> firestore.Client:
    try:
        cred = credentials.Certificate("key-prod.json")
        firebase_admin.initialize_app(cred)
        print("Firebase app initialized successfully")
    except ValueError as e:
        if "already exists" in str(e):
            print("Firebase app already initialized, continuing...")
        else:
            raise

    return firestore.client()


def main() -> None:
    db = init_firebase()

    now = datetime.now(timezone.utc)
    start = now - timedelta(days=3)

    print(f"Listing audit reports created between {start.isoformat()} and {now.isoformat()}")

    report_col = (
        db.collection("Tenant")
        .document(TENANT_ID)
        .collection("Forms")
        .document("audit")
        .collection("report")
    )

    query = (
        report_col.where("createdIn", ">=", start)
        .where("createdIn", "<=", now)
        .order_by("createdIn")
    )

    count = 0
    rows = []
    for doc in query.stream():
        data = doc.to_dict() or {}
        created_in = data.get("createdIn")
        user_cpf = data.get("userCpf")
        user_id = data.get("userId")
        classification = data.get("classification")

        print(
            f"- id={doc.id} createdIn={created_in} userId={user_id} userCpf={user_cpf} classification={classification}"
        )
        rows.append(
            {
                "id": doc.id,
                "createdIn": created_in,
                "userId": user_id,
                "userCpf": user_cpf,
                "classification": classification,
            }
        )
        count += 1

    print(f"Total docs: {count}")

    if pd is None:
        print("pandas is not installed; skipping DataFrame creation.")
        print("Install with: pip install pandas")
        return

    df = pd.DataFrame(rows)
    print("\nDataFrame preview:")
    print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()

