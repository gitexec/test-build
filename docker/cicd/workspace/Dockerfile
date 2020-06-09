FROM gcr.io/icentris-ml-${_ENV}/workspace:latest

WORKDIR $WORKSPACE

ENV ENV=${_ENV}

ENV GOOGLE_APPLICATION_CREDENTIALS=/home/app/.config/gcloud/cicd.json

COPY --chown=app:users src .

COPY --chown=app:users cicd.json /home/app/.config/gcloud

RUN gcloud config set project icentris-ml-${_ENV} && \
    gcloud auth activate-service-account icentris-cicd@icentris-ml-${_ENV}.iam.gserviceaccount.com --key-file=/home/app/.config/gcloud/cicd.json