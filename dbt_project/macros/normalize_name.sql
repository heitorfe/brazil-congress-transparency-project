{% macro normalize_name(col) %}
    upper(trim(
        replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
            coalesce({{ col }}, ''),
            'ã', 'a'), 'Ã', 'A'),
            'â', 'a'), 'Â', 'A'),
            'á', 'a'), 'Á', 'A'),
            'à', 'a'), 'À', 'A'),
            'é', 'e'), 'É', 'E'),
            'ê', 'e'), 'Ê', 'E'),
            'è', 'e'), 'È', 'E'),
            'í', 'i'), 'Í', 'I'),
            'î', 'i'), 'Î', 'I'),
            'ó', 'o'), 'Ó', 'O'),
            'ô', 'o'), 'Ô', 'O'),
            'õ', 'o'), 'Õ', 'O'),
            'ú', 'u'), 'Ú', 'U'),
            'û', 'u'), 'Û', 'U'),
            'ç', 'c'), 'Ç', 'C'),
            'ñ', 'n'), 'Ñ', 'N'),
            '.', ''),
            '-', ' '),
            '  ', ' ')
    ))
{% endmacro %}
