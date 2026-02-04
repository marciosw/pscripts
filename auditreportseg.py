import asyncio
import firebase_admin
import time
from firebase_admin import credentials, firestore
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
import threading
import gc
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

# Performance settings for 1M+ patients
BATCH_SIZE = 200  # Larger batches for better throughput
MAX_CONCURRENT_BATCHES = 100  # Maximum concurrent batch operations
MAX_WORKERS = 200  # Aggressive parallelization
CHUNK_SIZE = 1000  # Process UIDs in chunks

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
date_from = None
date_to = None

@dataclass
class ProcessingResult:
    uid: str
    cpf: str
    success: bool
    error: Optional[str] = None

def update_stats(key: str, value: int = 1):
    """Thread-safe stats update"""
    with stats_lock:
        stats[key] += value

def get_stats() -> Dict[str, int]:
    """Get current stats"""
    with stats_lock:
        return stats.copy()

def is_date_in_range(timestamp: Any) -> bool:
    """Check if timestamp is within the date range"""
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
    
    # Check if date is within range
    return date_from <= dt <= date_to

async def check_report_exists_async(uid: str) -> bool:
    """Check if report document already exists"""
    try:
        report_ref = await asyncio.to_thread(
            db.collection('Tenant')
            .document('vWxkIvCXIJUSIlhBSiIl')
            .collection('Forms')
            .document('audit')
            .collection('report')
            .document(uid)
            .get
        )
        return report_ref.exists
    except Exception:
        return False

async def verify_audit_async(uid: str) -> Tuple[bool, bool]:
    """Async verification of audit data with date filtering
    Returns: (has_audit_data, is_in_date_range)
    """
    try:
        # Use asyncio.to_thread for Firestore operations
        classif_ref = await asyncio.to_thread(
            db.collection('Tenant')
            .document('vWxkIvCXIJUSIlhBSiIl')
            .collection('Patient')
            .document(uid)
            .collection('e-tib-triagem-resultados')
            .document('classificacao')
            .get
        )
        
        if not classif_ref.exists:
            return False, False
        
        # Check date range if dates are provided
        classif_data = classif_ref.to_dict()
        timestamp = classif_data.get('timestamp') if classif_data else None
        
        in_range = is_date_in_range(timestamp)
        return True, in_range
    except Exception:
        return False, False

def process_questions_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Process question data to extract answers and selected responses"""
    answers = []
    selected = []
    
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

async def process_patient_async(uid: str) -> ProcessingResult:
    """Async patient processing for maximum performance with date filtering"""
    try:
        # Check if report already exists
        report_exists = await check_report_exists_async(uid)
        if report_exists:
            update_stats('already_exists', 1)
            return ProcessingResult(uid=uid, cpf='', success=False, error='report already exists')
        
        # Fetch all data in parallel using asyncio.gather
        classif_task = asyncio.to_thread(
            db.collection('Tenant')
            .document('vWxkIvCXIJUSIlhBSiIl')
            .collection('Patient')
            .document(uid)
            .collection('e-tib-triagem-resultados')
            .document('classificacao')
            .get
        )
        
        pat_task = asyncio.to_thread(
            db.collection('Tenant')
            .document('vWxkIvCXIJUSIlhBSiIl')
            .collection('Patient')
            .document(uid)
            .get
        )
        
        repostas_task = asyncio.to_thread(
            db.collection('Tenant')
            .document('vWxkIvCXIJUSIlhBSiIl')
            .collection('Patient')
            .document(uid)
            .collection('e-tib-triagem')
            .get
        )
        
        # Wait for all data to be fetched
        classif_ref, pat_ref, repostas = await asyncio.gather(
            classif_task, pat_task, repostas_task
        )

        if not repostas or len(repostas) <= 0:
            return ProcessingResult(uid=uid, cpf='', success=False, error='sem repostas')

        classif = classif_ref.to_dict()
        
        # Check date range before processing
        timestamp = classif.get('timestamp') if classif else None
        if not is_date_in_range(timestamp):
            return ProcessingResult(uid=uid, cpf='', success=False, error='outside date range')
        
        pat = pat_ref.to_dict()
        questions = []
        
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
        await asyncio.to_thread(
            db.collection('Tenant')
            .document('vWxkIvCXIJUSIlhBSiIl')
            .collection('Forms')
            .document('audit')
            .collection('report')
            .document(uid)
            .set,
            newobj
        )
        
        return ProcessingResult(
            uid=uid,
            cpf=pat.get('cpf', '') if pat else '',
            success=True
        )
        
    except Exception as error:
        return ProcessingResult(uid=uid, cpf='', success=False, error=str(error))

async def process_batch_async(uids: List[str]) -> Tuple[int, int, int]:
    """Process a batch of UIDs asynchronously"""
    valid_count = 0
    processed_count = 0
    failed_count = 0
    
    # Process all UIDs in the batch concurrently
    tasks = [process_patient_async(uid) for uid in uids]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    for result in results:
        if isinstance(result, Exception):
            failed_count += 1
        elif result.success:
            processed_count += 1
        else:
            failed_count += 1
    
    return valid_count, processed_count, failed_count

async def get_all_uids_async() -> List[str]:
    """Get all UIDs from Firestore Patient collection with isModera filter"""
    all_uids = []
    
    try:
        # Query Firestore for patients with isModera == True
        def fetch_patients():
            patients_ref = db.collection('Tenant') \
                .document('vWxkIvCXIJUSIlhBSiIl') \
                .collection('Patient') \
                .where('isModera', '==', True) \
                .stream()
            return list(patients_ref)
        
        patients = await asyncio.to_thread(fetch_patients)
        
        for patient_doc in patients:
            all_uids.append(patient_doc.id)
                
    except Exception as e:
        print(f"Error getting patients from Firestore: {e}")
        raise e
    
    return all_uids

async def main_async():
    """Main async function for high-performance processing with date filtering"""
    global date_from, date_to
    
    print("Starting high-performance audit processing with date filtering...")
    print(f"Date range: {date_from} to {date_to}")
    print(f"Batch size: {BATCH_SIZE}")
    print(f"Max concurrent batches: {MAX_CONCURRENT_BATCHES}")
    print(f"Max workers: {MAX_WORKERS}")
    
    # Get all UIDs from Firestore with isModera filter
    print("Fetching UIDs from Firestore (isModera == true)...")
    all_uids = await get_all_uids_async()
    print(f"Total UIDs found: {len(all_uids)}")
    
    # Verify UIDs and filter by date range
    print("Verifying UIDs and checking date range...")
    valid_uids = []
    
    # Process verification in batches
    for i in range(0, len(all_uids), BATCH_SIZE):
        chunk = all_uids[i:i + BATCH_SIZE]
        
        # Verify each UID in the chunk
        tasks = [verify_audit_async(uid) for uid in chunk]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        chunk_valid = []
        for uid, result in zip(chunk, results):
            if isinstance(result, Exception):
                continue
            has_audit, in_range = result
            if has_audit and in_range:
                chunk_valid.append(uid)
            else:
                update_stats('skipped', 1)
        
        valid_uids.extend(chunk_valid)
        update_stats('valid_uids', len(chunk_valid))
        
        if i % (BATCH_SIZE * 10) == 0:
            current_stats = get_stats()
            elapsed = time.time() - stats['start_time']
            print(f"Verified {i + len(chunk)}/{len(all_uids)} UIDs. "
                  f"Valid: {current_stats['valid_uids']}, "
                  f"Skipped: {current_stats['skipped']}, "
                  f"Rate: {i/elapsed:.0f} UIDs/sec")
    
    print(f"Verification complete. Valid UIDs: {len(valid_uids)}")
    
    # Process valid UIDs in batches
    print("Processing valid UIDs...")
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_BATCHES)
    
    async def process_batch_with_semaphore(uids):
        async with semaphore:
            return await process_batch_async(uids)
    
    # Create batches and process them concurrently
    batch_tasks = []
    for i in range(0, len(valid_uids), BATCH_SIZE):
        batch = valid_uids[i:i + BATCH_SIZE]
        task = process_batch_with_semaphore(batch)
        batch_tasks.append(task)
    
    # Process all batches concurrently
    batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
    
    # Aggregate results
    total_processed = 0
    total_failed = 0
    
    for result in batch_results:
        if isinstance(result, Exception):
            total_failed += BATCH_SIZE  # Assume entire batch failed
        else:
            _, processed, failed = result
            total_processed += processed
            total_failed += failed
    
    update_stats('processed', total_processed)
    update_stats('failed', total_failed)
    
    # Final statistics
    final_stats = get_stats()
    total_time = time.time() - stats['start_time']
    
    print(f"\n=== FINAL SUMMARY ===")
    print(f"Date range: {date_from} to {date_to}")
    print(f"Total UIDs from Firestore (isModera == true): {len(all_uids)}")
    print(f"Valid UIDs found (with audit data in date range): {final_stats['valid_uids']}")
    print(f"Successfully processed: {final_stats['processed']}")
    print(f"Failed to process: {final_stats['failed']}")
    print(f"Skipped (no audit data or outside date range): {final_stats['skipped']}")
    print(f"Already exists (skipped): {final_stats.get('already_exists', 0)}")
    print(f"Total execution time: {total_time:.2f} seconds")
    print(f"Average processing rate: {len(all_uids)/total_time:.2f} UIDs/second")
    print(f"Processing rate: {final_stats['processed']/total_time:.2f} patients/second")
    print("High-performance audit processing complete!")

def parse_date(date_string: str) -> datetime:
    """Parse date string in format YYYY-MM-DD or YYYY-MM-DD HH:MM:SS"""
    try:
        # Try parsing with time first
        if len(date_string) > 10:
            return datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")
        else:
            return datetime.strptime(date_string, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Invalid date format: {date_string}. Use YYYY-MM-DD or YYYY-MM-DD HH:MM:SS")

def main():
    """Main function that runs the async processing"""
    global date_from, date_to
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description='Process audit reports filtered by date range',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python auditreportseg.py --from-date 2024-01-01 --to-date 2024-01-31
  python auditreportseg.py --from-date "2024-01-01 00:00:00" --to-date "2024-01-31 23:59:59"
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
        
        # Validate date range
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
    
    # Set up asyncio event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        loop.run_until_complete(main_async())
    except KeyboardInterrupt:
        print("\nProcess interrupted by user")
    except Exception as e:
        print(f"Error in main processing: {e}")
    finally:
        loop.close()

if __name__ == "__main__":
    main()




# Exemplos de uso:
# # Filter by date range (full days)
# python auditreportseg.py --from-date 2026-02-02 --to-date 2026-02-04
#
# # Filter with specific times
# python auditreportseg.py --from-date "2024-01-01 00:00:00" --to-date "2024-01-31 23:59:59"