{#
  get_period_label macro
  Formats a year column and a month column into a 'YYYY-MM' string label.

  Usage:
    {{ get_period_label('invoice_year', 'invoice_month') }}
    -- returns: invoice_year::text || '-' || LPAD(invoice_month::text, 2, '0')
#}
{% macro get_period_label(year_col, month_col) %}
    {{ year_col }}::text || '-' || LPAD({{ month_col }}::text, 2, '0')
{% endmacro %}
