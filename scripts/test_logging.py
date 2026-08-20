from utils.logger import setup_logger


logger = setup_logger(
    "LoggingTest"
)


logger.info(
    "Pipeline logging system started."
)

logger.warning(
    "This is a test warning."
)

logger.error(
    "This is a test error."
)


print(
    "\nLogging test completed."
)