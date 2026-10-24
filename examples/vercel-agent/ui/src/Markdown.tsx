import { memo, type ComponentProps } from "react";
import { Streamdown } from "streamdown";

const components = {
  table: ({
    children,
    node: _node,
    ...props
  }: ComponentProps<"table"> & { node?: unknown }) => (
    <div
      className="markdown-table"
      role="region"
      aria-label="Scrollable results table"
      tabIndex={0}
    >
      <table {...props}>{children}</table>
    </div>
  ),
};

export default memo(function Markdown({
  text,
  streaming = false,
}: {
  text: string;
  streaming?: boolean;
}) {
  return (
    <Streamdown
      mode={streaming ? "streaming" : "static"}
      isAnimating={streaming}
      skipHtml
      disallowedElements={["img", "iframe", "script", "style"]}
      components={components}
      controls={false}
      className="markdown"
    >
      {text}
    </Streamdown>
  );
});
