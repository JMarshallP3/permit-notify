"""
Parsing Queue System for PermitTracker
Handles queuing, retry logic, and monitoring for permit parsing operations.
"""

import logging
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from enum import Enum
from dataclasses import dataclass, asdict
from pathlib import Path

logger = logging.getLogger(__name__)

class ParseStatus(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress" 
    SUCCESS = "success"
    FAILED = "failed"
    RETRY_QUEUED = "retry_queued"
    MANUAL_REVIEW = "manual_review"

class ParseStrategy(Enum):
    STANDARD = "standard"
    RETRY_FRESH_SESSION = "retry_fresh_session"
    ALTERNATIVE_PDF = "alternative_pdf"
    MANUAL_EXTRACTION = "manual_extraction"

@dataclass
class ParseJob:
    permit_id: str
    status_no: str
    attempt_count: int = 0
    max_attempts: int = 3
    status: ParseStatus = ParseStatus.PENDING
    strategy: ParseStrategy = ParseStrategy.STANDARD
    created_at: datetime = None
    last_attempt: datetime = None
    error_message: str = None
    confidence_score: float = 0.0
    parsed_fields: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
        if self.parsed_fields is None:
            self.parsed_fields = {}

class ParsingQueue:
    """
    Manages parsing queue with persistence and retry logic.
    """
    
    def __init__(self, queue_file: str = "parsing_queue.jsonl"):
        self.queue_file = Path(queue_file)
        self.jobs: Dict[str, ParseJob] = {}
        self.load_queue()
    
    def add_job(self, permit_id: str, status_no: str, strategy: ParseStrategy = ParseStrategy.STANDARD) -> ParseJob:
        """Add a new parsing job to the queue."""
        job = ParseJob(
            permit_id=permit_id,
            status_no=status_no,
            strategy=strategy
        )
        
        self.jobs[permit_id] = job
        self.save_queue()
        logger.info(f"Added parsing job for permit {permit_id} (status: {status_no})")
        return job
    
    def get_pending_jobs(self, limit: int = 10) -> List[ParseJob]:
        """Get pending jobs ready for processing."""
        pending = [
            job for job in self.jobs.values() 
            if job.status in [ParseStatus.PENDING, ParseStatus.RETRY_QUEUED]
            and job.attempt_count < job.max_attempts
        ]
        
        # Sort by creation time, oldest first
        pending.sort(key=lambda x: x.created_at)
        return pending[:limit]
    
    def update_job(self, permit_id: str, status: ParseStatus, error_message: str = None, 
                   parsed_fields: Dict[str, Any] = None, confidence_score: float = None):
        """Update job status and details."""
        if permit_id not in self.jobs:
            logger.warning(f"Job {permit_id} not found in queue")
            return
        
        job = self.jobs[permit_id]
        job.status = status
        job.last_attempt = datetime.now()
        
        if error_message:
            job.error_message = error_message
        
        if parsed_fields:
            job.parsed_fields = parsed_fields
            
        if confidence_score is not None:
            job.confidence_score = confidence_score
        
        # Increment attempt count for failed jobs
        if status == ParseStatus.FAILED:
            job.attempt_count += 1
            
            # Queue for retry if under max attempts
            if job.attempt_count < job.max_attempts:
                job.status = ParseStatus.RETRY_QUEUED
                # Use different strategy for retry
                if job.strategy == ParseStrategy.STANDARD:
                    job.strategy = ParseStrategy.RETRY_FRESH_SESSION
                elif job.strategy == ParseStrategy.RETRY_FRESH_SESSION:
                    job.strategy = ParseStrategy.ALTERNATIVE_PDF
                else:
                    job.status = ParseStatus.MANUAL_REVIEW
        
        self.save_queue()
        logger.info(f"Updated job {permit_id}: {status.value}")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get parsing queue statistics."""
        if not self.jobs:
            return {
                "total_jobs": 0,
                "pending": 0,
                "in_progress": 0,
                "success": 0,
                "failed": 0,
                "manual_review": 0,
                "success_rate": 0.0,
                "avg_confidence": 0.0
            }
        
        stats = {
            "total_jobs": len(self.jobs),
            "pending": 0,
            "in_progress": 0,
            "success": 0,
            "failed": 0,
            "manual_review": 0
        }
        
        confidence_scores = []
        
        for job in self.jobs.values():
            if job.status == ParseStatus.PENDING or job.status == ParseStatus.RETRY_QUEUED:
                stats["pending"] += 1
            elif job.status == ParseStatus.IN_PROGRESS:
                stats["in_progress"] += 1
            elif job.status == ParseStatus.SUCCESS:
                stats["success"] += 1
                if job.confidence_score > 0:
                    confidence_scores.append(job.confidence_score)
            elif job.status == ParseStatus.FAILED:
                stats["failed"] += 1
            elif job.status == ParseStatus.MANUAL_REVIEW:
                stats["manual_review"] += 1
        
        completed = stats["success"] + stats["failed"] + stats["manual_review"]
        stats["success_rate"] = (stats["success"] / completed * 100) if completed > 0 else 0.0
        stats["avg_confidence"] = sum(confidence_scores) / len(confidence_scores) if confidence_scores else 0.0
        
        return stats
    
    def get_failed_jobs(self, limit: int = 50) -> List[ParseJob]:
        """Get jobs that need manual review."""
        failed = [
            job for job in self.jobs.values() 
            if job.status in [ParseStatus.FAILED, ParseStatus.MANUAL_REVIEW]
        ]
        
        # Sort by last attempt, most recent first
        failed.sort(key=lambda x: x.last_attempt or x.created_at, reverse=True)
        return failed[:limit]
    
    def retry_job(self, permit_id: str) -> bool:
        """Manually retry a failed job."""
        if permit_id not in self.jobs:
            return False
        
        job = self.jobs[permit_id]
        if job.status not in [ParseStatus.FAILED, ParseStatus.MANUAL_REVIEW]:
            return False
        
        job.status = ParseStatus.RETRY_QUEUED
        job.attempt_count = 0  # Reset attempt count for manual retry
        job.strategy = ParseStrategy.STANDARD
        job.error_message = None
        
        self.save_queue()
        logger.info(f"Manually queued job {permit_id} for retry")
        return True
    
    def clean_old_jobs(self, days_old: int = 30):
        """Remove old completed jobs to keep queue manageable."""
        cutoff_date = datetime.now() - timedelta(days=days_old)
        
        to_remove = []
        for permit_id, job in self.jobs.items():
            if (job.status in [ParseStatus.SUCCESS, ParseStatus.FAILED] and 
                (job.last_attempt or job.created_at) < cutoff_date):
                to_remove.append(permit_id)
        
        for permit_id in to_remove:
            del self.jobs[permit_id]
        
        if to_remove:
            self.save_queue()
            logger.info(f"Cleaned {len(to_remove)} old jobs from parsing queue")
    
    def save_queue(self):
        """Save queue to disk."""
        try:
            with open(self.queue_file, 'w') as f:
                for job in self.jobs.values():
                    job_dict = asdict(job)
                    # Convert datetime objects to ISO strings
                    if job_dict['created_at']:
                        job_dict['created_at'] = job_dict['created_at'].isoformat()
                    if job_dict['last_attempt']:
                        job_dict['last_attempt'] = job_dict['last_attempt'].isoformat()
                    # Convert enums to strings
                    job_dict['status'] = job_dict['status'].value
                    job_dict['strategy'] = job_dict['strategy'].value
                    
                    f.write(json.dumps(job_dict) + '\n')
        except Exception as e:
            logger.error(f"Failed to save parsing queue: {e}")
    
    def load_queue(self):
        """Load queue from disk."""
        if not self.queue_file.exists():
            return
        
        try:
            with open(self.queue_file, 'r') as f:
                for line in f:
                    if line.strip():
                        job_dict = json.loads(line)
                        
                        # Convert ISO strings back to datetime objects
                        if job_dict['created_at']:
                            job_dict['created_at'] = datetime.fromisoformat(job_dict['created_at'])
                        if job_dict['last_attempt']:
                            job_dict['last_attempt'] = datetime.fromisoformat(job_dict['last_attempt'])
                        
                        # Convert strings back to enums
                        job_dict['status'] = ParseStatus(job_dict['status'])
                        job_dict['strategy'] = ParseStrategy(job_dict['strategy'])
                        
                        job = ParseJob(**job_dict)
                        self.jobs[job.permit_id] = job
                        
            logger.info(f"Loaded {len(self.jobs)} jobs from parsing queue")
        except Exception as e:
            logger.error(f"Failed to load parsing queue: {e}")
            self.jobs = {}

# Global parsing queue instance
parsing_queue = ParsingQueue()
