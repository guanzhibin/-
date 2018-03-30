#coding: utf8
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import app_setting
def send(receiver, title, body):
	# 第三方 SMTP 服务
	email_host=app_setting.email_host #设置服务器
	email_user=app_setting.email_user    #用户名
	email_pass=app_setting.email_pass   #口令 


	sender = app_setting.email_user

	message = MIMEText(body, _subtype=app_setting.subtype,_charset='utf-8')
	message['From'] = sender
	message['To'] =  ','.join(receiver)
	message['Subject'] = Header(title, 'utf-8')


	try:
		smtpObj = smtplib.SMTP(email_host,25)
		smtpObj.login(email_user,email_pass)
		smtpObj.sendmail(sender, receiver, message.as_string())
		smtpObj.close()
		return True
	except smtplib.SMTPException as e:
		return False

if __name__ == '__main__':
	receiver = []
	receiver.append('805115189@qq.com')
	title = '报警'
	body = '''
	<p>测试</p>
	'''
	a = send(receiver, title, body)
