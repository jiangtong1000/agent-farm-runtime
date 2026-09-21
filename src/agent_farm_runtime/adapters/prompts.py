"""Task-to-prompt rendering for agent executors, independent of project policy."""
from ..models import Task


def worker_contract(task: Task) -> str:
    text = ("\n\nRUNTIME TASK CONTRACT (effective acceptance, including recorded amendments):\n"
            f"Task: {task.id}\nAcceptance:\n{task.acceptance}\n"
            "A wake is a notification to re-evaluate the named condition, not evidence "
            "of success or acceptance. Follow the task's instructions; report AWAITING "
            "again when its prerequisites remain unmet.\n")
    instruction = task.metadata.get("latest_master_instruction")
    if instruction:
        text += (f"Latest recorded instruction ({instruction['ts']}, source {instruction['path']}):\n"
                 f"{instruction['text']}\n")
    surrender = task.metadata.get("clean_surrender")
    if surrender:
        checkpoint = surrender["checkpoint"]
        text += ("\nCONTINUATION CHECKPOINT (not acceptance; preserve holds):\n"
                 f"{checkpoint['text']}\n")
    return text
