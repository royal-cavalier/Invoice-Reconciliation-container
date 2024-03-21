FROM public.ecr.aws/lambda/python:3.11-x86_64

RUN yum install gcc -y

# Copy requirements.txt
COPY requirements.txt ${LAMBDA_TASK_ROOT}

RUN pip install --upgrade pip

# Install the specified packages
RUN pip install -r requirements.txt

# Copy function code
COPY lambda_function.py ${LAMBDA_TASK_ROOT}

COPY service_account.json ${LAMBDA_TASK_ROOT}

# Create an empty 'tmp' folder for lambda use 
RUN mkdir ${LAMBDA_TASK_ROOT}/tmp

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "lambda_function.handler" ]
