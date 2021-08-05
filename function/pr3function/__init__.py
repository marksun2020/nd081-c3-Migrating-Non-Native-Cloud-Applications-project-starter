from constants import SENDGRID_API_KEY

import logging
import psycopg2
import azure.functions as func
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def main(msg: func.ServiceBusMessage):

    logging.info('Python ServiceBus queue trigger processed message')
    notification_id = int(msg.get_body().decode('utf-8'))
    logging.info('Python ServiceBus queue trigger processed message: %s',notification_id)

    # TODO: Get connection to database
    dbConnection = psycopg2.connect( dbname="techconfdb", 
                                     user="marksun@pr3dbserver", 
                                     password="55_Sophia1", 
                                     host="pr3dbserver.postgres.database.azure.com" )
    dbCursor = dbConnection.cursor()
    try:
        logging.info('trying get notification message')
         
        dbCursor.execute("SELECT message, subject FROM notification where id = {};".format(notification_id))
        notification = dbCursor.fetchone()

        # TODO: Get attendees email and name
        dbCursor.execute("SELECT email, first_name, last_name FROM attendee;")
        attendees = dbCursor.fetchall()

        # TODO: Loop through each attendee and send an email with a personalized subject
        for attendee in attendees:
            callee = attendee[0]
            subject = "{}: {}".format(attendee[1], notification[1])
            logging.info("Send email to {} with message {}".format(callee, subject))
            # sendEmail(callee, subject, notification[0])

        now = datetime.utcnow()
        status = "Notified {} attendees".format(len(attendees))

        # TODO: Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        dbUpdateCommand = """update notification set status = '{}', completed_date= '{}' where id= {}""".format(status, now, notification_id)
        logging.info(dbUpdateCommand)
        dbCursor.execute(dbUpdateCommand)
        dbConnection.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
        dbConnection.rollback()
    finally:
        # TODO: Close connection
        dbCursor.close()
        dbConnection.close()

def sendEmail(email, subject, body):
    message = Mail(
        from_email="info@techconf.com",
        to_emails=email,
        subject=subject,
        plain_text_content=body)

    sg = SendGridAPIClient(SENDGRID_API_KEY) # the apikey here is the one provided in the config file of the project.
    sg.send(message)