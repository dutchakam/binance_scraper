from string import Template
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os


class EmailSender:
    contacts = r'path\to\contacts\file.txt'
    my_message = r'path\to\message\template.txt'
    my_address = 'email@site.com'
    my_pass = os.environ.get('P_EMAIL')
    s = smtplib.SMTP(host='smtp.mail.site.com', port=587)  # server
    msg = MIMEMultipart()  # message
    email_list = None  # list
    name_list = None  # list
    template = None  # string Template class
    df = None  # pandas DataFrame

    # def __init__(self, contacts, my_message, my_address, my_pass):
    #     self.contacts = contacts
    #     self.my_message = my_message
    #     self.my_address = my_address
    #     self.my_pass = my_pass

    def run_server(self):
        self.s.ehlo()
        self.s.starttls()
        # self.s.set_debuglevel(1)
        self.s.login(self.my_address, self.my_pass)

    def close_server(self):
        self.s.quit()

    def get_dataframe(self, df):
        self.df = df

    def get_contacts(self):
        name_lst = []
        email_lst = []
        with open(self.contacts, mode='r', encoding='utf-8') as email_list:
            for contact in email_list:
                name_lst.append(contact.split(': ')[0].strip())
                email_lst.append(contact.split(': ')[1].strip())
        self.name_list = name_lst
        self.email_list = email_lst

    def read_template(self):
        with open(self.my_message, 'r', encoding='utf-8') as template_file:
            template_file_content = template_file.read()
        self.template = Template(template_file_content)

    def compose_email(self):
        for name, email in zip(self.name_list, self.email_list):
            new_message = self.template.substitute(PERSON_NAME=name.title(), DATA_FRAME=self.df)

            # setup the parameters of the message
            self.msg['From'] = self.my_address
            self.msg['To'] = email
            self.msg['Subject'] = 'DAILY BINANCE SCRAPE RESULTS'

            # insert message body
            self.msg.attach(MIMEText(new_message, 'plain'))

    def send_email(self):
        self.s.send_message(self.msg)

