{% macro celsius_to_fahrenheit(column) -%}
    ({{ column }} * 9.0 / 5.0 + 32.0)
{%- endmacro %}


{#
  Corn growing degree days, modified 86/50 method: daily max and min are
  clamped to [50F, 86F] before averaging, then 50F is subtracted. Corn barely
  develops below 50F and stops speeding up above 86F.
#}
{% macro corn_gdd(tmin_f, tmax_f) -%}
    (
        (least(greatest({{ tmax_f }}, 50.0), 86.0) + least(greatest({{ tmin_f }}, 50.0), 86.0)) / 2.0
        - 50.0
    )
{%- endmacro %}
