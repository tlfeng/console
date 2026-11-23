import request, { formatResponse } from "@/utils/request";
import { cloneDeep } from "lodash";
import { useEffect, useLayoutEffect, useRef, useState } from "react";
import { formatTimeRange } from "@/lib/elasticsearch/util";
import { formatMessage } from "umi/locale";
import styles from "./Metrics.scss";
import { Alert, Dropdown, Empty, Icon, Menu, message, Spin, Tooltip } from "antd";
import {
    Axis,
    Chart,
    CurveType,
    LineSeries,
    BarSeries,
    niceTimeFormatByDay,
    Position,
    ScaleType,
    Settings,
    timeFormatter,
} from "@elastic/charts";
import { formatter, getFormatter, getNumFormatter } from "@/utils/format";
import { CopyToClipboard } from "react-copy-to-clipboard";
import moment from "moment";
import { ESPrefix } from "@/services/common";

export default (props) => {

    const { 
      timezone,
      timeRange,
      timeout,
      refresh,
      handleTimeChange,
      fetchUrl,
      metricKey,
      title,
      queryParams,
      className,
      style,
      formatMetric,
      height = 200,
      customRenderChart,
      instance,
      pointerUpdate,
      handleTimeIntervalChange,
      advancedTopN = false
    } = props;
  
    const [loading, setLoading] = useState(false)

    const [metric, setMetric] = useState()
  
    const [error, setError] = useState();
  
    const observerRef = useRef({ isInView: false })
  
    const containerRef = useRef(null)
  
    const firstFetchRef = useRef(true)
    const activeTopNRef = useRef()

    const [timeInterval, setTimeInterval] = useState()

    const { min: queryMin, max: queryMax, ...selection } = queryParams || {}
    // A relative range's expression, not its moving bounds, identifies a chart query.
    const chartQuery = JSON.stringify([
      fetchUrl, metricKey, timeInterval, timeout, advancedTopN, selection,
      timeRange?.min ?? queryMin, timeRange?.max ?? queryMax
    ])
    const querySignature = JSON.stringify(queryParams)

    const abortTopN = () => {
      const active = activeTopNRef.current;
      activeTopNRef.current = undefined; // Also discard its queued refresh.
      active?.controller.abort();
    }

    const fetchData = async (params, showLoading) => {
      if (!observerRef.current.isInView || !fetchUrl) return;
      const newQueryParams = { ...params, key: metricKey, timeout };
      if (timeInterval) newQueryParams.bucket_size = timeInterval;

      if (advancedTopN) {
        const previous = activeTopNRef.current;
        if (previous?.key === chartQuery) {
          // Replace rather than append: a burst of ticks/clicks owes only one fetch.
          previous.next = { params, showLoading };
          return;
        }
        previous?.controller.abort();
        const controller = new AbortController();
        const active = { key: chartQuery, controller };
        activeTopNRef.current = active;
        newQueryParams.advanced_topn = true;
        newQueryParams.topn_async = true;
        setError();
        if (firstFetchRef.current || showLoading) setLoading(true);

        let res;
        let pending = false;
        let deadline;
        let finishOnAbort;
        try {
          const query = async () => {
            while (!controller.signal.aborted) {
              const response = await request(fetchUrl, {
                method: 'GET',
                queryParams: newQueryParams,
                ignoreTimeout: true,
                signal: controller.signal,
                expirys: false
              }, false, false);
              if (controller.signal.aborted || activeTopNRef.current !== active) return;
              const status = response?.topn_status;
              if (status === 'pending') pending = true;
              if (status !== 'pending' && !(pending && status === 'busy')) return response;
              setLoading(true);
              setError(formatMessage({ id: `cluster.metrics.topn.${status}` }));
              // Once admitted, polls use exactly the same absolute window.
              await new Promise((resolve) => {
                const finish = () => {
                  clearTimeout(timer);
                  controller.signal.removeEventListener('abort', finish);
                  resolve();
                };
                const timer = setTimeout(finish, 2000);
                controller.signal.addEventListener('abort', finish, { once: true });
              });
            }
          };
          res = await Promise.race([
            new Promise((resolve) => {
              finishOnAbort = () => resolve();
              controller.signal.addEventListener('abort', finishOnAbort, { once: true });
              deadline = setTimeout(() => {
                resolve({ topn_status: 'timed_out' });
                controller.abort();
              }, 90000);
            }),
            query()
          ]);
        } catch {
          res = undefined;
        } finally {
          clearTimeout(deadline);
          if (finishOnAbort) controller.signal.removeEventListener('abort', finishOnAbort);
        }
        if (activeTopNRef.current !== active || !observerRef.current.isInView) return;
        activeTopNRef.current = undefined;
        firstFetchRef.current = false;
        const fail = (status) => {
          setMetric(undefined);
          setError(formatMessage({ id: `cluster.metrics.topn.${status}` }));
        };
        try {
          if (res?.topn_status) {
            fail(res.topn_status);
          } else if (res?.error || (res?.metrics?.[metricKey] && typeof res.metrics[metricKey] === 'object' && !Array.isArray(res.metrics[metricKey]))) {
            applyResponse(res);
          } else {
            fail('query_failed');
          }
        } catch {
          fail('query_failed');
        } finally {
          setLoading(false);
        }
        if (active.next) fetchData(active.next.params, active.next.showLoading);
        return;
      }

      if (firstFetchRef.current || showLoading) setLoading(true);
      const legacyContext = observerRef.current.legacyContext;
      let res;
      try {
        res = await request(fetchUrl, {
          method: 'GET',
          queryParams: newQueryParams,
          ignoreTimeout: true
        }, false, false);
      } catch {
        res = undefined;
      }
      // Legacy requests stay uncancelled, but must not write across a mode change.
      if (observerRef.current.legacyContext !== legacyContext) return;
      applyResponse(res);
      if (firstFetchRef.current || showLoading) {
        setLoading(false);
        firstFetchRef.current = false;
      }
    }

    const applyResponse = (res) => {
      if (res?.error) {
        const error = formatResponse(res);
        setError(error?.errorObject?.key ? formatMessage({ id: `${error?.errorObject?.key}` }) : res?.error?.reason);
      } else if (res) {
        const metric = res.metrics?.[metricKey];
        setError();
        setMetric(formatMetric && metric ? formatMetric(metric) : metric);
      }
    }

    useLayoutEffect(() => {
      observerRef.current.query = chartQuery;
      observerRef.current.advancedTopN = advancedTopN;
    }, [chartQuery, advancedTopN]);

    useLayoutEffect(() => {
      const observer = observerRef.current;
      observer.legacyContext = {};
      setMetric(undefined);
      setError();
      setLoading(false);
      firstFetchRef.current = true;
      return () => { observer.legacyContext = undefined; };
    }, [advancedTopN]);

    // Invalidate before I/O can report a result for an older committed query.
    useLayoutEffect(() => {
      if (advancedTopN) setMetric(undefined);
      return abortTopN;
    }, [chartQuery]);

    const fixFormat = (formatType, format) => {
      return formatType === 'num' && format ? `${format}a` : format
    }

    const currentParams = (params) => advancedTopN && timeRange?.min != null && timeRange?.max != null
      ? { ...params, ...formatTimeRange(timeRange) }
      : params;

    useEffect(() => {
      const observer = observerRef.current;
      const args = [cloneDeep(queryParams), refresh];
      const deps = { key: chartQuery, start: () => fetchData(currentParams(args[0]), args[1]) };
      observer.deps = deps;
      if (observer.isInView) {
        observer.lastDeps = deps;
        fetchData(...args);
      }
    }, [querySignature, chartQuery, refresh]);

    useEffect(() => {
      const state = observerRef.current;
      const observer = new IntersectionObserver(
        entries => {
          entries.forEach(entry => {
            if (entry.isIntersecting) {
              state.isInView = true;
              const { deps } = state;
              if (deps && deps.key === state.query && deps !== state.lastDeps) {
                state.lastDeps = deps;
                deps.start();
              }
            } else {
              state.isInView = false;
              if (state.advancedTopN) {
                abortTopN();
                state.lastDeps = undefined;
                setLoading(false);
              }
            }
          });
        },
        {
          root: null,
          threshold: 0,
        }
      );
      if (containerRef.current) observer.observe(containerRef.current);
      return () => observer.disconnect();
    }, []);

    const handleRefresh = () => fetchData(cloneDeep(currentParams(queryParams)), true);
  
    const handleChartBrush = ({ x }) => {
      if (!x) {
        return;
      }
      let [from, to] = x;
      if (typeof handleTimeChange == "function") {
        if (to - from < 20 * 1000) {
          from -= 10 * 1000;
          to += 10 * 1000;
        }
        handleTimeChange({
          start: moment(from).toISOString(),
          end: moment(to).toISOString(),
        });
      }
    };

    const renderChart = () => {
      if (error && (!advancedTopN || !loading || !metric)) {
        return (
          <div style={{ height, padding: 10 }}>
            <Alert style={{ maxHeight: '100%', overflow: 'auto', wordBreak: 'break-all'}} message={error} type={loading ? "info" : "error"}/>
          </div>
        )
      }
      const axis = metric?.axis || [];
      const lines = metric?.lines || [];
      if (lines.every((item) => !item.data || item.data.length === 0)) {
        const emptyProps = {}
        if (metric?.min_bucket_size > 0 && metric?.hits_total > 0) {
          emptyProps.description = (
            <>
              <div style={{ wordBreak: 'break-all', textAlign: 'left', marginBotton: 2 }} >
                {formatMessage({ id: "cluster.metrics.time_interval.empty" }, { min_bucket_size: metric.min_bucket_size})}
              </div>
              <Dropdown overlay={(
                <Menu>
                  <Menu.Item onClick={() => handleTimeIntervalChange(`${metric?.min_bucket_size}s`)}>
                    {formatMessage({ id: `cluster.metrics.time_interval.set.global`})}
                  </Menu.Item>
                  <Menu.Item onClick={() => setTimeInterval(`${metric?.min_bucket_size}s`)}>
                    {formatMessage({ id: `cluster.metrics.time_interval.set.current`})}
                  </Menu.Item>
                </Menu>
              )}>
                <a onClick={e => e.preventDefault()}>
                  {formatMessage({ id: `cluster.metrics.time_interval.apply`})} <Icon type="down" />
                </a>
              </Dropdown>
            </>
          )
        }
        return (
          <div style={{ height, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <Empty style={{ margin: 0}} image={Empty.PRESENTED_IMAGE_SIMPLE} {...emptyProps} />
          </div>
        )
      }
      if (customRenderChart) {
        return customRenderChart(metric)
      }
      return (
        <Chart
          size={[, height]}
          className={styles.vizChartItem}
          ref={instance}
        >
          <Settings
            pointerUpdateDebounce={0}
            pointerUpdateTrigger="x"
            onPointerUpdate={pointerUpdate}
            showLegend
            legendPosition={Position.Bottom}
            onBrushEnd={handleChartBrush}
            tooltip={{
              headerFormatter: ({ value }) =>
                    `${formatter.full_dates(value)}`,
            }}
            debug={false}
          />
          <Axis
            id={`${metricKey}-bottom`}
            position={Position.Bottom}
            labelFormat={timeRange.timeFormatter}
            tickFormat={timeRange.timeFormatter}
          />
          {metricKey == "cluster_health" ? (
            <Axis
              id="cluster_health"
              position={Position.Left}
              tickFormat={(d) => Number(d).toFixed(0) + "%"}
            />
          ) : null}
  
          {axis.map((item) => {
            return (
              <Axis
                key={metricKey + "-" + item.id}
                id={metricKey + "-" + item.id}
                showGridLines={item.showGridLines}
                groupId={item.group}
                position={item.position}
                ticks={item.ticks}
                labelFormat={getFormatter(
                  item.formatType,
                  fixFormat(item.formatType, item.tickFormat)
                )}
                tickFormat={getFormatter(
                  item.formatType,
                  fixFormat(item.formatType, item.tickFormat)
                )}
              />
            );
          })}
  
          {lines.map((item) => {
            if (item.type == "Bar") {
              return (
                <BarSeries
                  key={item.metric.label}
                  xScaleType={ScaleType.Time}
                  yScaleType={ScaleType.Linear}
                  xAccessor="x"
                  yAccessors={["y"]}
                  stackAccessors={["x"]}
                  splitSeriesAccessors={["g"]}
                  data={item.data}
                  color={({ specId, yAccessor, splitAccessors }) => {
                    const g = splitAccessors.get("g");
                    if (
                      yAccessor === "y"
                    
                    ) {
                      if( ["red", "yellow", "green"].includes(g)){
                        return g;
                      }
                      if(g == "online" || g == "available"){
                        return "green";
                      }
                      if(g == "offline" || g == "unavailable" || g == "N/A"){
                        return "gray";
                      }
                      
                    }
                    return null;
                  }}
                />
              );
            }
            return (
              <LineSeries
                key={item.metric.label}
                id={item.metric.label}
                groupId={item.metric.group}
                timeZone={timezone}
                color={item.color}
                xScaleType={ScaleType.Time}
                yScaleType={ScaleType.Linear}
                xAccessor={0}
                tickFormat={getFormatter(
                  item.metric.formatType,
                  fixFormat(item.metric.formatType, item.metric.format),
                  item.metric.units
                )}
                yAccessors={[1]}
                data={item.data}
                curve={CurveType.CURVE_MONOTONE_X}
              />
            );
          })}
        </Chart>
      )
    }

    const chartTitle = { title };
    const lines = metric?.lines || [];
    if (metricKey == "cluster_health") {
      chartTitle.units = "%";
    } else {
      if (lines[0]?.metric) {
        if (lines[0].metric.formatType.toLowerCase == "bytes") {
          chartTitle.units = lines[0].metric.formatType;
        } else {
          chartTitle.units = lines[0].metric.units;
        }
      }
    }
  
    const heading = (
        <div className={styles.vizChartItemTitle}>
          <span>
            {chartTitle.title}
            {chartTitle.units ? `(${chartTitle.units})` : ""}
          </span>
          {
            <span>
              {
                timeInterval && (
                  <Tooltip title={formatMessage({id: "cluster.metrics.time_interval.reload"}, { time_interval: queryParams.bucket_size })}>
                    <Icon className={styles.copy} style={{ marginRight: 12 }} type="history" onClick={() => setTimeInterval()}/>
                  </Tooltip>
                )
              }
              <Tooltip title={formatMessage({id: "form.button.refresh"})}>
                <Icon className={styles.copy} type="sync" onClick={handleRefresh}/>
              </Tooltip>
            </span>
          }
        </div>
    );

    return (
      <div key={metricKey} ref={containerRef} className={className} style={style}>
        {metric?.request && (
          <CopyToClipboard text={`GET .infini_metrics/_search\n${metric.request}`}>
            <Tooltip title={formatMessage({id: "cluster.metrics.request.copy"})}>
              <Icon
                className="copyReq"
                type="copy"
                onClick={() => message.success(formatMessage({id: "cluster.metrics.request.copy.success"}))}
              />
            </Tooltip>
          </CopyToClipboard>
        )}
        {advancedTopN && heading}
        <Spin spinning={loading}>
          {!advancedTopN && heading}
          {renderChart()}
        </Spin>
      </div>
    );
  }