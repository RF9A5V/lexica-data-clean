export function markerCheck(data) {
  if (!data.content) return null;
  const markerPattern = /^\(([a-zA-Z0-9ivxlcdmIVXLCDM]+)\)/;
  if (markerPattern.test(data.content)) {
    return `Marker still present at start of content: '${data.content.slice(0, 20)}...'`;
  }
  return null;
}
