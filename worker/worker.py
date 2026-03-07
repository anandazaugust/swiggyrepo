import json
import logging
import pyodbc
import sys
import time

from azure.servicebus import ServiceBusClient
from azure.identity import DefaultAzureCredential

from config import settings


# Configure logging to output to stdout for kubectl logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)


# Create credential once (reused everywhere)
credential = DefaultAzureCredential()


def get_sql_connection():
    """
    Establishes connection to Azure SQL using Managed Identity token.
    This respects AZURE_CLIENT_ID automatically.
    """
    logger.info("Attempting to open SQL connection...")

    try:
        token = credential.get_token("https://database.windows.net/.default")

        # SQL expects UTF-16 encoded token
        token_bytes = token.token.encode("utf-16-le")

        conn = pyodbc.connect(
            settings.sql_connection_string,
            attrs_before={1256: token_bytes},
            timeout=60
        )

        logger.info("SQL Connection established successfully.")
        return conn

    except pyodbc.Error as e:
        logger.error(f"DATABASE CONNECTION ERROR: {e}")
        raise


def save_to_db(order):
    """
    Inserts the parsed order into the SQL database.
    """
    conn = None

    try:
        order_id = order.get("orderId")
        customer_id = order.get("customerId")
        created_at = order.get("createdAt")

        logger.info(f"Inserting order {order_id} into DB")

        conn = get_sql_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO Orders (OrderId, CustomerId, CreatedAt)
            VALUES (?, ?, ?)
            """,
            order_id,
            customer_id,
            created_at
        )

        conn.commit()

        logger.info(f"Successfully committed Order {order_id} to database.")

    except Exception as e:
        logger.error(f"Failed to save to DB: {str(e)}")
        raise

    finally:
        if conn:
            conn.close()
            logger.info("SQL connection closed.")


def process_order(message):
    """
    Parses the Service Bus message and triggers the DB save.
    """

    try:
        # Correct Service Bus body parsing
        body = b"".join(message.body).decode("utf-8")

        logger.info(f"Processing Raw Message: {body}")

        order = json.loads(body)

        if not order.get("orderId"):
            raise ValueError("Message body missing orderId")

        save_to_db(order)

    except json.JSONDecodeError:
        logger.error("Invalid JSON format in message")
        raise

    except Exception as e:
        logger.error(f"Processing logic failed: {e}")
        raise


def main():
    """
    Main loop that stays alive and listens to the Service Bus.
    """

    logger.info("Worker starting up...")

    try:
        logger.info("Creating Service Bus client using Managed Identity...")

        client = ServiceBusClient(
            fully_qualified_namespace=settings.service_bus_namespace,
            credential=credential
        )

        with client:

            receiver = client.get_queue_receiver(
                queue_name=settings.service_bus_queue_name
            )

            logger.info(
                f"Worker is now listening on queue: {settings.service_bus_queue_name}"
            )

            with receiver:

                for message in receiver:

                    try:
                        logger.info(f"Received message: {message.message_id}")

                        process_order(message)

                        receiver.complete_message(message)

                        logger.info(
                            f"Message {message.message_id} completed."
                        )

                    except Exception:

                        logger.error(
                            f"Worker encountered an error processing message "
                            f"{message.message_id}. Abandoning message for retry."
                        )

                        receiver.abandon_message(message)

                        # prevent rapid crash loop
                        time.sleep(2)

    except Exception as e:

        logger.critical(f"FATAL ERROR: Worker process crashed: {e}")

        sys.exit(1)


if __name__ == "__main__":
    main()