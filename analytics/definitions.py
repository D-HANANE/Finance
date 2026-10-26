from dagster import Definitions, ScheduleDefinition, DefaultScheduleStatus, job, op, RetryPolicy
from gold import run


@op(retry_policy=RetryPolicy(max_retries=2, delay=30))
def gold_and_quality(context):
    context.log.info(run())


@job
def refresh_gold():
    gold_and_quality()


defs = Definitions(jobs=[refresh_gold], schedules=[
    ScheduleDefinition(job=refresh_gold, cron_schedule='*/5 * * * *', execution_timezone='UTC',
                       name='gold_every_five_minutes', default_status=DefaultScheduleStatus.RUNNING),
    ScheduleDefinition(job=refresh_gold, cron_schedule='5 0 * * *', execution_timezone='UTC',
                       name='end_of_day', default_status=DefaultScheduleStatus.RUNNING),
])
