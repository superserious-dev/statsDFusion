const METRICS_QUERY_FORM_ID = "metricsQueryForm";
const CHART_CONTAINER_ID = "chart";

const metricsQueryForm = document.getElementById(METRICS_QUERY_FORM_ID);
const chartContainer = document.getElementById(CHART_CONTAINER_ID);

metricsQueryForm.addEventListener("submit", function (e) {
  e.preventDefault();

  const formData = new FormData(metricsQueryForm);
  const params = new URLSearchParams(formData);

  query(params);
});

async function query(params) {
  const flushedAtStart = new Date(params.get("flushed-at-start"));
  const flushedAtEnd = new Date(params.get("flushed-at-end"));

  params.set("flushed-at-start", flushedAtStart.toISOString());
  params.set("flushed-at-end", flushedAtEnd.toISOString());

  try {
    const response = await fetch(`/query?${params.toString()}`, {
      method: "GET",
      headers: {
        "Content-Type": "application/json",
        "X-Requested-With": "XMLHttpRequest",
      },
    });

    if (response.ok) {
      // NOTE incoming data is sorted flushed at ASC
      const result = await response.json();
      console.log("Success:", result); // FIXME add visual feedback while chart loading
      buildChart(result.flushes, flushedAtStart, flushedAtEnd);
    } else {
      throw new Error(`HTTP Error... status: ${response.status}`);
    }
  } catch (error) {
    console.error(error);
  }
}

/// A metric is identified by it's name and tags
function buildMetricIdentifier(metric) {
  const { name, tags = {} } = metric;
  const tagEntries = Object.entries(tags);

  if (tagEntries.length == 0) {
    return name;
  }

  const tagStrings = tagEntries.map(([key, value]) =>
    value != null ? `${key}=${value}` : key,
  );

  return `${name} [ ${tagStrings.join(", ")} ]`;
}

function aggregateFlushes(flushes) {
  let flushedAts = [];
  let metricsMap = new Map();

  for (const flush of flushes) {
    const flushedAt = new Date(flush.flushed_at);
    flushedAts.push(flushedAt);

    // Aggregate if there are metrics in the flush
    // NOTE flush.metrics being `[null]` means that no metrics were recorded in the interval
    // TODO investigate how to return [] instead of [null]
    if (!(flush.metrics.length === 1 && flush.metrics[0] === null)) {
      for (const metric of flush.metrics) {
        const identifier = buildMetricIdentifier(metric);

        // If this is the first time seeing the metric, create the flushed at => value map
        if (!metricsMap.has(identifier)) {
          metricsMap.set(identifier, new Map());
        }

        metricsMap.get(identifier).set(flushedAt, metric.value);
      }
    }
  }

  return { flushedAts, metricsMap };
}

function createChartSeries(flushedAts, metricsMap) {
  let series = [];
  for (const [name, flushedAtMap] of metricsMap) {
    let data = flushedAts.map((flushedAt) => {
      const value = flushedAtMap.get(flushedAt) ?? "-";
      return [flushedAt, value];
    });
    series.push({
      type: "line",
      step: "end",
      showSymbol: false,
      name,
      data,
      sampling: "lttb",
    });
  }

  return series;
}

function createChartOptions(flushedAtStart, flushedAtEnd, series) {
  return {
    legend: {
      type: "scroll",
      orient: "horizontal",
      top: "top",
    },
    tooltip: {
      trigger: "axis",
      axisPointer: {
        animation: false,
      },
    },
    xAxis: {
      name: "Flushed At",
      nameLocation: "middle",
      type: "time",
      axisTick: { show: false },
      min: flushedAtStart,
      max: flushedAtEnd,
    },
    yAxis: {
      name: "Value",
      nameLocation: "middle",
      type: "value",
      axisTick: { show: false },
    },
    series,
  };
}

function buildChart(flushes, flushedAtStart, flushedAtEnd) {
  const metricsLineChart = echarts.init(chartContainer);
  const { flushedAts, metricsMap } = aggregateFlushes(flushes);
  const series = createChartSeries(flushedAts, metricsMap);

  const chartOptions = createChartOptions(flushedAtStart, flushedAtEnd, series);
  metricsLineChart.setOption(chartOptions, { notMerge: true });
}
