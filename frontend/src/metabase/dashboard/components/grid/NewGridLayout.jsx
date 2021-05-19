/* eslint-disable react/prop-types */
import React, { useCallback, useMemo, useState } from "react";
import ReactGridLayout from "react-grid-layout";
import _ from "underscore";

import Select from "metabase/components/Select";
import { color } from "metabase/lib/colors";

import "react-grid-layout/css/styles.css";
import "react-resizable/css/styles.css";

function NewGridLayout({
  items,
  itemRenderer,
  layout,
  cols,
  width: gridWidth,
  margin,
  rowHeight,
  isEditing,
  ...props
}) {
  const [compactType, setCompactType] = useState("vertical");

  const cellSize = useMemo(() => {
    const marginSlotsCount = cols - 1;
    const totalHorizontalMargin = marginSlotsCount * margin;
    const freeSpace = gridWidth - totalHorizontalMargin;
    return {
      width: freeSpace / cols,
      height: rowHeight,
    };
  }, [cols, gridWidth, rowHeight, margin]);

  const background = useMemo(() => {
    const XMLNS = "http://www.w3.org/2000/svg";
    const rowHeight = cellSize.height + margin;
    const cellStrokeColor = color("border");

    const y = 0;
    const w = cellSize.width;
    const h = cellSize.height;

    const rectangles = _(cols).times(i => {
      const x = i * (cellSize.width + margin);
      return `<rect stroke='${cellStrokeColor}' stroke-width='1' fill='none' x='${x}' y='${y}' width='${w}' height='${h}'/>`;
    });

    const svg = [
      `<svg xmlns='${XMLNS}' width='${gridWidth}' height='${rowHeight}'>`,
      ...rectangles,
      `</svg>`,
    ].join("");

    return `url("data:image/svg+xml;utf8,${encodeURIComponent(svg)}")`;
  }, [cellSize, gridWidth, margin, cols]);

  const renderItem = useCallback(
    item => {
      const itemLayout = layout.find(l => String(l.i) === String(item.id));
      const gridItemWidth = cellSize.width * itemLayout.w - margin;
      return itemRenderer({
        item,
        gridItemWidth,
      });
    },
    [layout, cellSize, margin, itemRenderer],
  );

  const height = useMemo(() => {
    let lowestLayoutCellPoint = Math.max(...layout.map(l => l.y + l.h));
    if (isEditing) {
      lowestLayoutCellPoint += Math.ceil(window.innerHeight / cellSize.height);
    }
    return (cellSize.height + margin) * lowestLayoutCellPoint;
  }, [cellSize.height, layout, margin, isEditing]);

  const style = useMemo(
    () => ({
      width: gridWidth,
      height,
      background: isEditing ? background : "",
    }),
    [gridWidth, height, background, isEditing],
  );

  return (
    <React.Fragment>
      {isEditing && (
        <div
          style={{
            position: "absolute",
            top: 5,
            left: 5,
            padding: "8px",
            borderRadius: "8px",
            zIndex: 100,
          }}
        >
          <Select
            value={compactType}
            options={[
              { value: "vertical", name: "Vertical" },
              { value: "horizontal", name: "Horizontal" },
              { value: null, name: "None" },
            ]}
            onChange={e => {
              const val = e.target.value;
              setCompactType(val);
            }}
          />
        </div>
      )}
      <ReactGridLayout
        cols={cols}
        layout={layout}
        width={gridWidth}
        margin={[margin, margin]}
        rowHeight={rowHeight}
        isDraggable={isEditing}
        isResizable={isEditing}
        {...props}
        autoSize={false}
        style={style}
        compactType={compactType}
      >
        {items.map(renderItem)}
      </ReactGridLayout>
    </React.Fragment>
  );
}

export default NewGridLayout;
