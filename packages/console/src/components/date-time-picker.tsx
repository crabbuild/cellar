// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { useState } from "react"
import { format, isValid, parseISO, startOfToday } from "date-fns"
import { CalendarIcon } from "lucide-react"
import { cn } from "cn"
import { Button } from "@/components/ui/button"
import { Calendar } from "@/components/ui/calendar"
import { Input } from "@/components/ui/input"
import {
  Field,
  FieldDescription,
  FieldGroup,
  FieldLabel,
  FieldLegend,
  FieldSet,
} from "@/components/ui/field"
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover"

export function DateTimePicker({
  id,
  label,
  value,
  onChange,
}: {
  id: string
  label: string
  value: string
  onChange: (value: string) => void
}) {
  const [open, setOpen] = useState(false)
  // Keep incomplete date/time edits intact until both fields have been entered.
  const [datePart = "", timePart = ""] = value.split("T")
  const parsed = datePart ? parseISO(datePart) : undefined
  const selected = parsed && isValid(parsed) ? parsed : undefined
  const today = startOfToday()
  const timeZone = Intl.DateTimeFormat().resolvedOptions().timeZone

  return (
    <FieldSet aria-describedby={`${id}-timezone`}>
      <FieldLegend variant="label">{label} date and time</FieldLegend>
      <FieldGroup className="flex-row gap-3">
        <Field className="min-w-0 flex-1">
          <FieldLabel htmlFor={`${id}-date`}>{label} date</FieldLabel>
          <Popover open={open} onOpenChange={setOpen}>
            <PopoverTrigger asChild>
              <Button
                id={`${id}-date`}
                type="button"
                variant="outline"
                className="w-full justify-between"
                aria-describedby={`${id}-timezone`}
              >
                <span
                  className={cn(
                    "truncate",
                    !selected && "text-muted-foreground"
                  )}
                >
                  {selected ? format(selected, "MMM d, yyyy") : "Pick a date"}
                </span>
                <CalendarIcon data-icon="inline-end" aria-hidden />
              </Button>
            </PopoverTrigger>
            <PopoverContent
              align="start"
              className="w-auto max-w-[calc(100vw-1.5rem)] p-0"
              collisionPadding={12}
              aria-label={`${label} calendar`}
            >
              <Calendar
                mode="single"
                required
                autoFocus
                captionLayout="dropdown"
                defaultMonth={selected ?? today}
                selected={selected}
                startMonth={today}
                endMonth={
                  new Date(
                    Math.max(
                      today.getFullYear() + 100,
                      selected?.getFullYear() ?? 0
                    ),
                    11
                  )
                }
                disabled={{ before: today }}
                onSelect={(date) => {
                  onChange(`${format(date, "yyyy-MM-dd")}T${timePart}`)
                  setOpen(false)
                }}
              />
            </PopoverContent>
          </Popover>
        </Field>
        <Field className="w-36 shrink-0">
          <FieldLabel htmlFor={`${id}-time`}>{label} time</FieldLabel>
          <Input
            id={`${id}-time`}
            type="time"
            step="1"
            value={timePart}
            aria-describedby={`${id}-timezone`}
            onChange={(event) => onChange(`${datePart}T${event.target.value}`)}
            className="[&::-webkit-calendar-picker-indicator]:hidden"
          />
        </Field>
      </FieldGroup>
      <FieldDescription id={`${id}-timezone`}>
        Local time · {timeZone}. Review shows UTC.
      </FieldDescription>
    </FieldSet>
  )
}
