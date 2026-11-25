FROM grafana/grafana:11.6.0
USER root
RUN mkdir -p /opt/grafana-plugins && chown grafana:root /opt/grafana-plugins
USER grafana
RUN grafana cli --pluginsDir /opt/grafana-plugins plugins install grafana-clickhouse-datasource 4.8.2
ENV GF_PATHS_PLUGINS=/opt/grafana-plugins
