# OpenSearch + analysis-ik plugin for the local e2e verification harness.
# The IK plugin version MUST match the OpenSearch version.
ARG OPENSEARCH_VERSION=2.17.0
FROM opensearchproject/opensearch:${OPENSEARCH_VERSION}

ARG OPENSEARCH_VERSION=2.17.0
# infinilabs hosts analysis-ik builds for OpenSearch.
RUN /usr/share/opensearch/bin/opensearch-plugin install --batch \
    https://release.infinilabs.com/analysis-ik/stable/opensearch-analysis-ik-${OPENSEARCH_VERSION}.zip
