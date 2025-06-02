import smtplib
from email.mime.text import MIMEText

smtp_host = 'smtp.gmail.com'
smtp_port = 587
smtp_user = 'annakhripkovafbst@gmail.com'
smtp_password = 'krhl qwjs pmjh pbah'

msg = MIMEText('Тестовое письмо')
msg['Subject'] = 'Тест'
msg['From'] = smtp_user
msg['To'] = 'annakhripkovafbst@gmail.com'

try:
    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(smtp_user, ['annakhripkovafbst@gmail.com'], msg.as_string())
    print("Письмо отправлено успешно!")
except Exception as e:
    print(f"Ошибка: {e}")