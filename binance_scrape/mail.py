import smtplib
from string import Template
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class Mailer:
    def __init__(self, username, password, host, port):  # initialize new Mailer class instance
        self.username = username
        self.password = password
        self.host = host
        self.port = port

    def send_email(self, subject, send_from, send_to, content, df):
        template = Template(content)  # organize content into a string template
        name = send_to.split('@')[0]
        new_message = template.substitute(PERSON_NAME=name.title(), DATA_FRAME=df)  # fill in template params

        msg = MIMEMultipart('alternative')  # set up message
        msg['Subject'] = subject
        msg['To'] = send_to
        msg['From'] = send_from

        text = MIMEText(new_message, 'plain')  # prepare message text
        msg.attach(text)  # attach message text

        s = smtplib.SMTP(host=self.host, port=self.port)  # run smtp server
        s.ehlo()
        s.starttls()
        s.ehlo()
        # self.s.set_debuglevel(1)
        s.login(self.username, self.password)
        s.send_message(msg)
        print('\nEmail sent successfully!\n')
        s.quit()

