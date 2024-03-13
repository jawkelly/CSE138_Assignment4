# Using python base image
FROM python:3.11

# Set working directory to /app
WORKDIR /app

# Add everything in current directory to container's working directory
COPY . .

# Dependencies
RUN pip install requests apscheduler Flask

EXPOSE 8090

CMD ["python", "jack_kvsservice.py"]

