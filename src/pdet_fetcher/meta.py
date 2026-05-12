# Brazil UFs
states = [
    "AC",
    "AL",
    "AM",
    "AP",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MG",
    "MS",
    "MT",
    "PA",
    "PB",
    "PE",
    "PI",
    "PR",
    "RJ",
    "RN",
    "RO",
    "RR",
    "RS",
    "SC",
    "SE",
    "SP",
    "TO",
]

BASE_PATH = "/pdet/microdados"

uf_pattern = "|".join(states).lower()
month_pattern = "|".join(f"{m:02}" for m in range(1, 13))
year_pattern = r"\d{4}"

datasets = {
    "caged": {
        "variations": (
            {
                "path": BASE_PATH + "/CAGED",
                "dir_pattern": rf"^({year_pattern})$",
                "dir_pattern_groups": ("year",),
                "fn_pattern": rf"^cagedest_({month_pattern})({year_pattern})\.7z$",
                "fn_pattern_groups": ("month", "year"),
            },
        ),
    },
    "caged-ajustes": {
        "variations": (
            {
                "path": BASE_PATH + "/CAGED_AJUSTES/2002a2009",
                "dir_pattern": None,
                "dir_pattern_groups": None,
                "fn_pattern": rf"^cagedest_ajustes_({year_pattern})\.7z$",
                "fn_pattern_groups": ("year",),
            },
            {
                "path": BASE_PATH + "/CAGED_AJUSTES",
                "dir_pattern": rf"^({year_pattern})$",
                "dir_pattern_groups": ("year",),
                "fn_pattern": rf"^cagedest_ajustes_({month_pattern})({year_pattern})\.7z$",
                "fn_pattern_groups": ("month", "year"),
            },
        ),
    },
    "caged-2020-exc": {
        "variations": (
            {
                "path": BASE_PATH + "/NOVO CAGED",
                "dir_pattern": (
                    rf"^({year_pattern})$",
                    rf"^({year_pattern})({month_pattern})$",
                ),
                "dir_pattern_groups": (("year",), ("year", "month")),
                "fn_pattern": rf"^cagedexc({year_pattern})({month_pattern})\.7z$",
                "fn_pattern_groups": ("year", "month"),
            },
        ),
    },
    "caged-2020-for": {
        "variations": (
            {
                "path": BASE_PATH + "/NOVO CAGED",
                "dir_pattern": (
                    rf"^({year_pattern})$",
                    rf"^({year_pattern})({month_pattern})$",
                ),
                "dir_pattern_groups": (("year",), ("year", "month")),
                "fn_pattern": rf"^cagedfor({year_pattern})({month_pattern})\.7z$",
                "fn_pattern_groups": ("year", "month"),
            },
        ),
    },
    "caged-2020-mov": {
        "variations": (
            {
                "path": BASE_PATH + "/NOVO CAGED",
                "dir_pattern": (
                    rf"^({year_pattern})$",
                    rf"^({year_pattern})({month_pattern})$",
                ),
                "dir_pattern_groups": (("year",), ("year", "month")),
                "fn_pattern": rf"^cagedmov({year_pattern})({month_pattern})\.7z$",
                "fn_pattern_groups": ("year", "month"),
            },
        ),
    },
    "rais-vinculos": {
        "variations": (
            {
                "path": BASE_PATH + "/RAIS",
                "dir_pattern": rf"^({year_pattern})$",
                "dir_pattern_groups": ("year",),
                "fn_pattern": rf"^({uf_pattern})({year_pattern})\.7z$",
                "fn_pattern_groups": ("region", "year"),
            },
            {
                "path": BASE_PATH + "/RAIS",
                "dir_pattern": rf"^({year_pattern})$",
                "dir_pattern_groups": ("year",),
                "fn_pattern": rf"^ignora(|n)do(|s)({year_pattern})\.7z$",
                "fn_pattern_groups": (None, None, "year"),
            },
            {
                "path": BASE_PATH + "/RAIS",
                "dir_pattern": rf"^({year_pattern})$",
                "dir_pattern_groups": ("year",),
                "fn_pattern": r"^rais_vinc_pub_(centro_oeste|mg_es_rj|nordeste|norte|sp|sul)\.7z$",
                "fn_pattern_groups": ("region",),
            },
        ),
    },
    "rais-estabelecimentos": {
        "variations": (
            {
                "path": BASE_PATH + "/RAIS",
                "dir_pattern": rf"^({year_pattern})$",
                "dir_pattern_groups": ("year",),
                "fn_pattern": rf"^estb({year_pattern})\.(7z|zip)$",
                "fn_pattern_groups": ("year", "extension"),
            },
            {
                "path": BASE_PATH + "/RAIS",
                "dir_pattern": rf"^({year_pattern})$",
                "dir_pattern_groups": ("year",),
                "fn_pattern": r"^rais_estab_pub\.7z$",
                "fn_pattern_groups": (),
            },
        ),
    },
}

docs = {
    "rais-vinculos": {
        "dir_path": BASE_PATH + "/RAIS/Layouts/vínculos",
        "fn_pattern": r"^.*\.(xls|xlsx|pdf|txt)$",
    },
    "rais-estabelecimentos": {
        "dir_path": BASE_PATH + "/RAIS/Layouts/estabelecimento",
        "fn_pattern": r"^.*\.(xls|xlsx|pdf|txt)$",
    },
    "caged": {
        "dir_path": BASE_PATH + "/CAGED",
        "fn_pattern": r"^.*\.(xls|xlsx|pdf|txt)$",
    },
    "caged-ajustes": {
        "dir_path": BASE_PATH + "/CAGED_AJUSTES",
        "fn_pattern": r"^.*\.(xls|xlsx|pdf|txt)$",
    },
    "caged-2020": {
        "dir_path": BASE_PATH + "/NOVO CAGED",
        "fn_pattern": r"^.*\.(xls|xlsx|pdf|txt)$",
    },
}
