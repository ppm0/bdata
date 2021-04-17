FROM python:3.8
WORKDIR /app
ADD *.py /app/
ADD requirements.txt /app/
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "bdata.py", "--help"]