##################################################################
# Base image
##################################################################
# ARG BUILDTAG
# FROM alpine:latest as base
FROM frolvlad/alpine-python3 as base

WORKDIR /src

COPY . ./

RUN apk upgrade --update-cache \ 
    && pip3 install -e .

RUN apk add --no-cache gcc

RUN apk upgrade --update-cache \
    && apk add --no-cache --virtual .build_dependencies musl-dev \
    && pip3 install --no-cache-dir -r requirements.txt \
    && apk del .build_dependencies

##################################################################
# Builder
##################################################################
FROM base as builder

RUN apk --no-cache add make \
    && pip3 install -e .[dev] \
    && make clean lint scan test

##################################################################
# Runtime
##################################################################
FROM base as release

# clean up src and then oonly copy in the executable code

COPY --from=builder /src/app app
COPY --from=builder /src/venv venv
COPY --from=builder /src/scripts/entrypoint.sh .

# Serve http on port 5000
EXPOSE 5000

ENTRYPOINT ["/bin/sh", "entrypoint.sh"]