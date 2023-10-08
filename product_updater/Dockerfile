FROM python:3

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY product_updater.py ./

# CMD ["python", "product_updater.py"]