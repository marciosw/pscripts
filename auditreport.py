import asyncio
import firebase_admin
import time
from firebase_admin import credentials, firestore, auth
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
import threading
import gc

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
    'start_time': time.time()
}
stats_lock = threading.Lock()

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

async def verify_audit_batch_async(uids: List[str]) -> List[str]:
    """Async batch verification for maximum performance"""
    valid_uids = []
    
    # Create tasks for parallel verification
    tasks = []
    for uid in uids:
        task = asyncio.create_task(verify_audit_async(uid))
        tasks.append((uid, task))
    
    # Wait for all tasks to complete
    for uid, task in tasks:
        try:
            if await task:
                valid_uids.append(uid)
        except Exception:
            pass  # Skip failed verifications
    
    return valid_uids

async def verify_audit_async(uid: str) -> bool:
    """Async verification of audit data"""
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
        return classif_ref.exists
    except Exception:
        return False

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
    """Async patient processing for maximum performance"""
    try:
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
    """Get all UIDs from Firebase Auth asynchronously"""
    all_uids = []
    page_token = None
    
    while True:
        try:
            # Use asyncio.to_thread for auth operations
            users = await asyncio.to_thread(
                auth.list_users, max_results=1000, page_token=page_token
            )
            
            for user in users.users:
                all_uids.append(user.uid)
            
            page_token = users.next_page_token
            if not page_token:
                break
                
        except Exception as e:
            print(f"Error getting users: {e}")
            break
    
    return all_uids

async def main_async():
    """Main async function for high-performance processing"""
    print("Starting high-performance audit processing...")
    print(f"Batch size: {BATCH_SIZE}")
    print(f"Max concurrent batches: {MAX_CONCURRENT_BATCHES}")
    print(f"Max workers: {MAX_WORKERS}")
    
    # Get all UIDs first
    print("Fetching all UIDs from Firebase Auth...")
    all_uids = await get_all_uids_async()
    print(f"Total UIDs found: {len(all_uids)}")
    
    # Verify UIDs in batches
    print("Verifying UIDs in batches...")
    valid_uids = []
    
    for i in range(0, len(all_uids), BATCH_SIZE):
        chunk = all_uids[i:i + BATCH_SIZE]
        chunk_valid = await verify_audit_batch_async(chunk)
        valid_uids.extend(chunk_valid)
        update_stats('valid_uids', len(chunk_valid))
        update_stats('skipped', len(chunk) - len(chunk_valid))
        
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
    print(f"Total UIDs processed: {len(all_uids)}")
    print(f"Valid UIDs found: {final_stats['valid_uids']}")
    print(f"Successfully processed: {final_stats['processed']}")
    print(f"Failed to process: {final_stats['failed']}")
    print(f"Skipped (no audit data): {final_stats['skipped']}")
    print(f"Total execution time: {total_time:.2f} seconds")
    print(f"Average processing rate: {len(all_uids)/total_time:.2f} UIDs/second")
    print(f"Processing rate: {final_stats['processed']/total_time:.2f} patients/second")
    print("High-performance audit processing complete!")

def main():
    """Main function that runs the async processing"""
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

