FROM python:3

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# ENTRYPOINT [ "sh", "-c", "python wait_for_mysql.py && python product_updater.py"]