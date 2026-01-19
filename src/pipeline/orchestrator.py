import logging
from typing import Any, List, Dict, Optional
from datetime import datetime
from enum import Enum

from pipeline.etl_processor import BaseETLProcessor, ETLConfig


class PipelineStatus(Enum):
    """Pipeline execution status"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class PipelineTask:
    """Represents a single task in pipeline"""
    
    def __init__(
        self,
        task_id: str,
        processor: BaseETLProcessor,
        config: ETLConfig,
        dependencies: Optional[List[str]] = None
    ):
        self.task_id = task_id
        self.processor = processor
        self.config = config
        self.dependencies = dependencies or []
        self.status = PipelineStatus.PENDING
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.error_message: Optional[str] = None
    
    def execute(self) -> bool:
        """Execute the task"""
        self.status = PipelineStatus.RUNNING
        self.start_time = datetime.now()
        
        success = self.processor.run(self.config)
        
        self.end_time = datetime.now()
        self.status = PipelineStatus.SUCCESS if success else PipelineStatus.FAILED
        
        return success
    
    def duration(self) -> Optional[float]:
        """Get task duration in seconds"""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None


class ETLOrchestrator:
    """Orchestrates ETL pipeline execution"""
    
    def __init__(self):
        self.tasks: Dict[str, PipelineTask] = {}
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def add_task(self, task: PipelineTask):
        """Add task to pipeline"""
        self.tasks[task.task_id] = task
        self.logger.info(f"Added task: {task.task_id}")
    
    def _check_dependencies(self, task: PipelineTask) -> bool:
        """Check if all dependencies are completed successfully"""
        for dep_id in task.dependencies:
            dep_task = self.tasks.get(dep_id)
            if not dep_task or dep_task.status != PipelineStatus.SUCCESS:
                return False
        return True
    
    def _get_ready_tasks(self) -> List[PipelineTask]:
        """Get tasks ready to execute"""
        ready = []
        for task in self.tasks.values():
            if task.status == PipelineStatus.PENDING and self._check_dependencies(task):
                ready.append(task)
        return ready
    
    def execute(self) -> Dict[str, PipelineStatus]:
        """Execute all tasks respecting dependencies"""
        self.logger.info("Starting pipeline execution")
        
        while True:
            ready_tasks = self._get_ready_tasks()
            
            if not ready_tasks:
                # Check if all tasks are done
                pending = [t for t in self.tasks.values() if t.status == PipelineStatus.PENDING]
                if not pending:
                    break  # All done
                else:
                    # Deadlock - dependencies not met
                    self.logger.error("Pipeline deadlock detected")
                    for task in pending:
                        task.status = PipelineStatus.SKIPPED
                    break
            
            # Execute ready tasks
            for task in ready_tasks:
                self.logger.info(f"Executing task: {task.task_id}")
                success = task.execute()
                
                if success:
                    self.logger.info(
                        f"Task {task.task_id} completed in {task.duration():.2f}s"
                    )
                else:
                    self.logger.error(f"Task {task.task_id} failed")
        
        # Return execution summary
        return {task_id: task.status for task_id, task in self.tasks.items()}
    
    def get_execution_summary(self) -> Dict[str, Any]:
        """Get detailed execution summary"""
        total = len(self.tasks)
        success = sum(1 for t in self.tasks.values() if t.status == PipelineStatus.SUCCESS)
        failed = sum(1 for t in self.tasks.values() if t.status == PipelineStatus.FAILED)
        skipped = sum(1 for t in self.tasks.values() if t.status == PipelineStatus.SKIPPED)
        
        total_duration = sum(
            t.duration() for t in self.tasks.values() if t.duration() is not None
        )
        
        return {
            "total_tasks": total,
            "successful": success,
            "failed": failed,
            "skipped": skipped,
            "total_duration": total_duration,
            "tasks": {
                task_id: {
                    "status": task.status.value,
                    "duration": task.duration(),
                    "start_time": task.start_time,
                    "end_time": task.end_time
                }
                for task_id, task in self.tasks.items()
            }
        }