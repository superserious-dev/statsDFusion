const METRICS_QUERY_FORM_ID = "metricsQueryForm";
const CHART_CONTAINER_ID = "chart";

const metricsQueryForm = document.getElementById(METRICS_QUERY_FORM_ID);
const chartContainer = document.getElementById(CHART_CONTAINER_ID);

metricsQueryForm.addEventListener("submit", function (e) {
  e.preventDefault();

  const formData = new FormData(metricsQueryForm);
  const params = new URLSearchParams(formData);

  const flushedAtStart = new Date(params.get("flushed-at-start"));
  const flushedAtEnd = new Date(params.get("flushed-at-end"));
  params.set("flushed-at-start", flushedAtStart.toISOString());
  params.set("flushed-at-end", flushedAtEnd.toISOString());

  query(params, flushedAtStart, flushedAtEnd);
});

async function query(params, flushedAtStart, flushedAtEnd) {
  try {
    const response = await fetch(`/query?${params.toString()}`, {
      method: "GET",
      headers: {
        "Content-Type": "application/json",
        "X-Requested-With": "XMLHttpRequest",
      },
    });

    if (response.ok) {
      const result = await response.json();
      console.log("Success:", result); // FIXME add visual feedback while chart loading
      buildChart(result.metrics, flushedAtStart, flushedAtEnd);
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

function groupMetricsByIdentifier(metrics) {
  return metrics.reduce((dataMap, metric) => {
    const identifier = buildMetricIdentifier(metric);
    const timestamp = new Date(metric.flushed_at);

    if (!dataMap.has(identifier)) {
      dataMap.set(identifier, []);
    }
    dataMap.get(identifier).push([timestamp, metric.value]);

    return dataMap;
  }, new Map());
}

function createChartSeries(liveData) {
  return Array.from(liveData.entries()).map(([name, data]) => ({
    type: "line",
    step: "end",
    showSymbol: false,
    name,
    data,
  }));
}

function createChartOptions(series, flushedAtStart, flushedAtEnd) {
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
      type: "time",
      axisTick: { show: false },
      min: flushedAtStart,
      max: flushedAtEnd,
    },
    yAxis: {
      name: "Value",
      type: "value",
      axisTick: { show: false },
    },
    series,
  };
}

function buildChart(metrics, flushedAtStart, flushedAtEnd) {
  const liveChart = echarts.init(chartContainer);
  const liveData = groupMetricsByIdentifier(metrics);
  const series = createChartSeries(liveData);
  const chartOptions = createChartOptions(series, flushedAtStart, flushedAtEnd);
  liveChart.setOption(chartOptions, { notMerge: true });
}
