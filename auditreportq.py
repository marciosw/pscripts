import firebase_admin
import time
from firebase_admin import credentials, firestore
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import threading
import queue
import argparse
from datetime import datetime

# Firebase initialization
try:
    cred = credentials.Certificate('key-prod.json')
    firebase_admin.initialize_app(cred)
    print("Firebase app initialized successfully")
except ValueError as e:
    if 'already exists' in str(e):
        print("Firebase app already initialized, continuing...")
    else:
        raise e

db = firestore.client()

# Performance settings for 1M+ patients (aggressive multithreading)
MAX_WORKERS = 200  # Maximum number of worker threads

# Global counters
stats = {
    'valid_uids': 0,
    'processed': 0,
    'failed': 0,
    'skipped': 0,
    'already_exists': 0,
    'start_time': time.time()
}
stats_lock = threading.Lock()

# Global date range for filtering
date_from: Optional[datetime] = None
date_to: Optional[datetime] = None


@dataclass
class ProcessingResult:
    uid: str
    cpf: str
    success: bool
    error: Optional[str] = None


def update_stats(key: str, value: int = 1) -> None:
    """Thread-safe stats update."""
    with stats_lock:
        stats[key] += value


def get_stats() -> Dict[str, int]:
    """Get current stats snapshot."""
    with stats_lock:
        return stats.copy()


def is_date_in_range(timestamp: Any) -> bool:
    """Check if timestamp is within the global date range."""
    global date_from, date_to

    if date_from is None or date_to is None:
        return True  # No filtering if dates not provided

    if timestamp is None:
        return False

    # Convert timestamp to datetime if it's a Firestore timestamp
    if hasattr(timestamp, 'timestamp'):
        dt = datetime.fromtimestamp(timestamp.timestamp())
    elif isinstance(timestamp, (int, float)):
        dt = datetime.fromtimestamp(timestamp)
    elif isinstance(timestamp, datetime):
        dt = timestamp
    else:
        return False

    return date_from <= dt <= date_to


def check_report_exists(uid: str) -> bool:
    """Check if report document already exists (synchronous)."""
    try:
        report_ref = (
            db.collection('Tenant')
            .document('vWxkIvCXIJUSIlhBSiIl')
            .collection('Forms')
            .document('audit')
            .collection('report')
            .document(uid)
            .get()
        )
        return report_ref.exists
    except Exception:
        return False


def process_questions_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Process question data to extract answers and selected responses."""
    answers: List[Any] = []
    selected: List[Any] = []

    if 'entradas' in data and data['entradas']:
        entradas_valores = list(data['entradas'].values())
        answers.extend(entradas_valores)
    elif 'alternativas' in data and data['alternativas']:
        alternativas_valores = list(data['alternativas'].values())
        for q in alternativas_valores:
            answers.append({
                "body": q.get('descricao', ''),
                "formId": q.get('descricao', ''),
                "index": q.get('index', 0)
            })

    if 'respostas' in data:
        selected.extend(data['respostas'])

    return {
        "selectedAnswers": selected,
        "question": {
            "answers": answers,
            "title": data.get('titulo', ''),
            "caption": data.get('subtitulo', '-'),
            "formId": "audit",
            "formQuestionId": data.get('id', ''),
        }
    }


def process_patient(uid: str) -> ProcessingResult:
    """Synchronous patient processing optimized for multithread + queues."""
    try:
        # Check if report already exists
        if check_report_exists(uid):
            update_stats('already_exists', 1)
            return ProcessingResult(uid=uid, cpf='', success=False, error='report already exists')

        # Fetch patient-related documents
        classif_ref = (
            db.collection('Tenant')
            .document('vWxkIvCXIJUSIlhBSiIl')
            .collection('Patient')
            .document(uid)
            .collection('e-tib-triagem-resultados')
            .document('classificacao')
            .get()
        )

        pat_ref = (
            db.collection('Tenant')
            .document('vWxkIvCXIJUSIlhBSiIl')
            .collection('Patient')
            .document(uid)
            .get()
        )

        repostas = (
            db.collection('Tenant')
            .document('vWxkIvCXIJUSIlhBSiIl')
            .collection('Patient')
            .document(uid)
            .collection('e-tib-triagem')
            .get()
        )

        if not repostas or len(repostas) <= 0:
            update_stats('skipped', 1)
            return ProcessingResult(uid=uid, cpf='', success=False, error='sem repostas')

        classif = classif_ref.to_dict()

        # Check date range before processing
        timestamp = classif.get('timestamp') if classif else None
        if not is_date_in_range(timestamp):
            update_stats('skipped', 1)
            return ProcessingResult(uid=uid, cpf='', success=False, error='outside date range')

        # At this point, UID is valid for processing
        update_stats('valid_uids', 1)

        pat = pat_ref.to_dict()
        questions: List[Dict[str, Any]] = []

        # Process questions
        for item in repostas:
            data = item.to_dict()
            data['id'] = item.id
            processed_question = process_questions_data(data)
            questions.append(processed_question)

        # Create the new object
        newobj = {
            "createdIn": classif.get('timestamp') if classif else None,
            "classification": classif.get('bpe') if classif else None,
            "userCpf": pat.get('cpf') if pat else None,
            "userId": pat_ref.id,
            "choosedAnswers": questions
        }

        # Save to Firestore
        (
            db.collection('Tenant')
            .document('vWxkIvCXIJUSIlhBSiIl')
            .collection('Forms')
            .document('audit')
            .collection('report')
            .document(uid)
            .set(newobj)
        )

        return ProcessingResult(
            uid=uid,
            cpf=pat.get('cpf', '') if pat else '',
            success=True
        )

    except Exception as error:
        return ProcessingResult(uid=uid, cpf='', success=False, error=str(error))


def get_all_uids() -> List[str]:
    """Get all UIDs from Firestore Patient collection with isModera filter."""
    all_uids: List[str] = []
    try:
        patients_ref = (
            db.collection('Tenant')
            .document('vWxkIvCXIJUSIlhBSiIl')
            .collection('Patient')
            .where('isModera', '==', True)
            .stream()
        )
        for patient_doc in patients_ref:
            all_uids.append(patient_doc.id)
    except Exception as e:
        print(f"Error getting patients from Firestore: {e}")
        raise e

    return all_uids


def worker(uid_queue: "queue.Queue[str]") -> None:
    """Worker function that consumes UIDs from the queue and processes them."""
    while True:
        uid = uid_queue.get()
        if uid is None:
            uid_queue.task_done()
            break

        result = process_patient(uid)

        if result.success:
            update_stats('processed', 1)
        else:
            # already_exists and skipped are updated inside process_patient
            # only count as failed when it's not a controlled skip
            if result.error not in ('report already exists', 'sem repostas', 'outside date range'):
                update_stats('failed', 1)

        uid_queue.task_done()


def parse_date(date_string: str) -> datetime:
    """Parse date string in format YYYY-MM-DD or YYYY-MM-DD HH:MM:SS."""
    try:
        if len(date_string) > 10:
            return datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")
        else:
            return datetime.strptime(date_string, "%Y-%m-%d")
    except ValueError:
        raise ValueError(
            f"Invalid date format: {date_string}. Use YYYY-MM-DD or YYYY-MM-DD HH:MM:SS"
        )


def main() -> None:
    """Main entry point using queues + multithreading."""
    global date_from, date_to

    parser = argparse.ArgumentParser(
        description='Process audit reports filtered by date range (queue + multithread version)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python auditreportq.py --from-date 2024-01-01 --to-date 2024-01-31
  python auditreportq.py --from-date "2024-01-01 00:00:00" --to-date "2024-01-31 23:59:59"
        """
    )
    parser.add_argument(
        '--from-date',
        type=str,
        required=True,
        help='Start date in format YYYY-MM-DD or YYYY-MM-DD HH:MM:SS'
    )
    parser.add_argument(
        '--to-date',
        type=str,
        required=True,
        help='End date in format YYYY-MM-DD or YYYY-MM-DD HH:MM:SS'
    )

    args = parser.parse_args()

    # Parse dates
    try:
        date_from = parse_date(args.from_date)
        date_to = parse_date(args.to_date)

        if date_from > date_to:
            print("Error: Start date must be before or equal to end date")
            return

        # Set time to start of day for from_date if not specified
        if len(args.from_date) <= 10:
            date_from = date_from.replace(hour=0, minute=0, second=0, microsecond=0)

        # Set time to end of day for to_date if not specified
        if len(args.to_date) <= 10:
            date_to = date_to.replace(hour=23, minute=59, second=59, microsecond=999999)

    except ValueError as e:
        print(f"Error parsing dates: {e}")
        return

    # Reset start time for this run
    with stats_lock:
        stats['start_time'] = time.time()

    print("Starting high-performance audit processing with queues + multithreading...")
    print(f"Date range: {date_from} to {date_to}")
    print(f"Max workers (threads): {MAX_WORKERS}")

    # Get all UIDs
    print("Fetching UIDs from Firestore (isModera == true)...")
    all_uids = get_all_uids()
    total_uids = len(all_uids)
    print(f"Total UIDs found: {total_uids}")

    if total_uids == 0:
        print("No UIDs to process. Exiting.")
        return

    # Prepare queue and workers
    uid_queue: "queue.Queue[str]" = queue.Queue()

    # Fill queue as fast as possible (producer)
    for uid in all_uids:
        uid_queue.put(uid)

    # Use as many threads as possible up to MAX_WORKERS
    num_workers = min(MAX_WORKERS, total_uids)
    print(f"Spawning {num_workers} worker threads...")

    threads: List[threading.Thread] = []
    for i in range(num_workers):
        t = threading.Thread(target=worker, args=(uid_queue,), name=f"worker-{i}", daemon=True)
        t.start()
        threads.append(t)

    # Progress monitoring loop
    last_report_time = time.time()
    try:
        while True:
            # Break when all tasks are done
            if uid_queue.unfinished_tasks == 0:
                break

            now = time.time()
            if now - last_report_time >= 10:  # report every 10 seconds
                current_stats = get_stats()
                elapsed = now - current_stats['start_time']
                processed = current_stats['processed'] + current_stats['skipped'] + current_stats.get('already_exists', 0)
                rate = processed / elapsed if elapsed > 0 else 0
                print(
                    f"Progress: {processed}/{total_uids} processed "
                    f"(valid: {current_stats['valid_uids']}, "
                    f"skipped: {current_stats['skipped']}, "
                    f"already exists: {current_stats['already_exists']}, "
                    f"failed: {current_stats['failed']}), "
                    f"rate: {rate:.2f} patients/sec"
                )
                last_report_time = now

            time.sleep(1)

        # Ensure all queue tasks are fully marked done
        uid_queue.join()

    except KeyboardInterrupt:
        print("\nProcess interrupted by user, stopping workers...")

    finally:
        # Stop workers with sentinel values
        for _ in range(num_workers):
            uid_queue.put(None)

        for t in threads:
            t.join()

    # Final statistics
    final_stats = get_stats()
    total_time = time.time() - final_stats['start_time']
    total_processed = final_stats['processed']

    print(f"\n=== FINAL SUMMARY ===")
    print(f"Date range: {date_from} to {date_to}")
    print(f"Total UIDs from Firestore (isModera == true): {total_uids}")
    print(f"Valid UIDs found (with audit data in date range): {final_stats['valid_uids']}")
    print(f"Successfully processed: {final_processed := final_stats['processed']}")
    print(f"Failed to process: {final_stats['failed']}")
    print(f"Skipped (no audit data or outside date range): {final_stats['skipped']}")
    print(f"Already exists (skipped): {final_stats.get('already_exists', 0)}")
    print(f"Total execution time: {total_time:.2f} seconds")
    print(f"Average processing rate: {total_uids/total_time:.2f} UIDs/second")
    print(f"Processing rate: {final_processed/total_time:.2f} patients/second")
    print("High-performance audit processing with queues + multithreading complete!")


if __name__ == "__main__":
    main()





# python auditreportq.py --from-date 2026-01-26 --to-date 2026-01-28