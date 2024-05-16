# pthon version
FROM python:3.12

# the directory where the code will be stored inside docker container
WORKDIR /code

# 
COPY ./requirements.txt /code/requirements.txt

# 
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

# 
COPY ./app /code/app

# run app in prod
CMD ["python", "app/main.py", "--docker"]