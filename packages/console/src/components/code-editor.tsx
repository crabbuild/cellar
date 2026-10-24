import {
  forwardRef,
  useEffect,
  useImperativeHandle,
  useLayoutEffect,
  useRef,
} from "react"
import { Compartment, EditorState } from "@codemirror/state"
import {
  EditorView,
  drawSelection,
  highlightActiveLine,
  highlightActiveLineGutter,
  keymap,
  lineNumbers,
} from "@codemirror/view"
import {
  defaultKeymap,
  history,
  historyKeymap,
  indentWithTab,
} from "@codemirror/commands"
import {
  HighlightStyle,
  bracketMatching,
  indentOnInput,
  syntaxHighlighting,
} from "@codemirror/language"
import {
  autocompletion,
  closeBrackets,
  closeBracketsKeymap,
  completionKeymap,
} from "@codemirror/autocomplete"
import { SQLite, sql, type SQLNamespace } from "@codemirror/lang-sql"
import { json } from "@codemirror/lang-json"
import { tags } from "@lezer/highlight"

export type CodeEditorHandle = {
  insert: (value: string) => void
  query: (selection?: boolean) => string
  focus: () => void
}

const highlighting = HighlightStyle.define([
  { tag: [tags.keyword, tags.operatorKeyword], color: "var(--info)" },
  { tag: [tags.string, tags.special(tags.string)], color: "var(--success)" },
  { tag: [tags.number, tags.bool, tags.null], color: "var(--warning)" },
  { tag: tags.comment, color: "var(--muted-foreground)", fontStyle: "italic" },
  { tag: [tags.punctuation, tags.operator], color: "var(--muted-foreground)" },
])
const theme = EditorView.theme({
  "&": {
    height: "100%",
    color: "var(--foreground)",
    backgroundColor: "var(--card)",
    fontSize: "var(--text-body)",
  },
  "&.cm-focused": { outline: "none" },
  ".cm-scroller": {
    overflow: "auto",
    fontFamily: "var(--font-mono)",
    lineHeight: "1.8",
  },
  ".cm-content": {
    padding: "var(--space-3) 0",
    caretColor: "var(--foreground)",
    minHeight: "100%",
  },
  ".cm-gutters": {
    color: "var(--muted-foreground)",
    backgroundColor: "var(--background)",
    borderRight: "1px solid var(--border)",
  },
  ".cm-line": { padding: "0 var(--space-3)" },
  ".cm-activeLine, .cm-activeLineGutter": { backgroundColor: "var(--muted)" },
  ".cm-selectionBackground, &.cm-focused .cm-selectionBackground": {
    backgroundColor: "var(--info-muted)",
  },
  ".cm-cursor": { borderLeftColor: "var(--foreground)" },
  ".cm-tooltip": {
    backgroundColor: "var(--popover)",
    color: "var(--popover-foreground)",
    border: "1px solid var(--border)",
    borderRadius: "var(--radius)",
    boxShadow: "var(--shadow-floating)",
  },
  ".cm-tooltip-autocomplete > ul > li[aria-selected]": {
    backgroundColor: "var(--info-muted)",
    color: "var(--info)",
  },
  ".cm-search": { backgroundColor: "var(--background)" },
})

export const CodeEditor = forwardRef<
  CodeEditorHandle,
  {
    value: string
    onChange: (value: string) => void
    label: string
    language?: "sql" | "json" | "text"
    schema?: SQLNamespace
    readOnly?: boolean
    onRun?: (query: string) => void
  }
>(
  (
    {
      value,
      onChange,
      label,
      language = "sql",
      schema,
      readOnly = false,
      onRun,
    },
    ref
  ) => {
    const root = useRef<HTMLDivElement>(null)
    const editor = useRef<EditorView | null>(null)
    const callbacks = useRef({ onChange, onRun })
    const initial = useRef(value)
    const compartment = useRef(new Compartment())
    useLayoutEffect(() => {
      callbacks.current = { onChange, onRun }
      initial.current = value
    }, [onChange, onRun, value])
    useImperativeHandle(
      ref,
      () => ({
        query: (selection = true) => {
          const state = editor.current?.state
          if (!state) return ""
          const range = state.selection.main
          return selection && !range.empty
            ? state.sliceDoc(range.from, range.to)
            : state.doc.toString()
        },
        insert: (text) => {
          const view = editor.current
          if (!view) return
          view.dispatch(view.state.replaceSelection(text))
          view.focus()
        },
        focus: () => editor.current?.focus(),
      }),
      []
    )
    useEffect(() => {
      if (!root.current) return
      const run = (view: EditorView, selected: boolean) => {
        if (!callbacks.current.onRun) return false
        const range = view.state.selection.main
        callbacks.current.onRun(
          selected && !range.empty
            ? view.state.sliceDoc(range.from, range.to)
            : view.state.doc.toString()
        )
        return true
      }
      const view = new EditorView({
        parent: root.current,
        state: EditorState.create({
          doc: initial.current,
          extensions: [
            lineNumbers(),
            highlightActiveLineGutter(),
            drawSelection(),
            highlightActiveLine(),
            history(),
            bracketMatching(),
            indentOnInput(),
            closeBrackets(),
            autocompletion(),
            keymap.of([
              { key: "Mod-Enter", run: (view) => run(view, true) },
              { key: "Mod-Shift-Enter", run: (view) => run(view, false) },
              ...closeBracketsKeymap,
              ...completionKeymap,
              ...defaultKeymap,
              ...historyKeymap,
              indentWithTab,
            ]),
            theme,
            syntaxHighlighting(highlighting),
            EditorView.lineWrapping,
            EditorView.contentAttributes.of({
              "aria-label": label,
              spellcheck: "false",
            }),
            EditorState.readOnly.of(readOnly),
            EditorView.editable.of(!readOnly),
            compartment.current.of([]),
            EditorView.updateListener.of((update) => {
              if (update.docChanged)
                callbacks.current.onChange(update.state.doc.toString())
            }),
          ],
        }),
      })
      editor.current = view
      return () => {
        editor.current = null
        view.destroy()
      }
    }, [label, readOnly])
    useEffect(() => {
      editor.current?.dispatch({
        effects: compartment.current.reconfigure(
          language === "sql"
            ? sql({ dialect: SQLite, schema })
            : language === "json"
              ? json()
              : []
        ),
      })
    }, [language, schema, label, readOnly])
    useEffect(() => {
      const view = editor.current
      if (view && value !== view.state.doc.toString())
        view.dispatch({
          changes: { from: 0, to: view.state.doc.length, insert: value },
        })
    }, [value])
    return <div ref={root} className="code-editor" />
  }
)
CodeEditor.displayName = "CodeEditor"
