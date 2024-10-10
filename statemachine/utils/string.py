import math
import re


def camel_to_snake(s: str) -> str:
    """
    Transforms a camel type cased string (e.g. "MyClass") to snake cased type (e.g. "my_class")

    Args:
        s: the string to be converted

    Returns:
        s_conv: converted string
    """
    return re.sub(r"(?<!^)(?=[A-Z])", "_", s).lower()


def prettify(n, precision=1) -> str:
    """
    Makes number display nicely. For ex, 1000 will be displayed as '1.0 K'.

    Args:
        n: the number to prettify
        precision: number of decimals kept

    Returns:
        s: the string representing the formatted number
    """
    millnames = ["", " K", " M", " B", " T"]

    if math.isnan(n):
        return "N/A"
    else:
        n = float(n)
        millidx = max(
            0,
            min(
                len(millnames) - 1,
                int(math.floor(0 if n == 0 else math.log10(abs(n)) / 3)),
            ),
        )
        str_container = "{:." + f"{precision}" + "f}{}"
        return str_container.format(n / 10 ** (3 * millidx), millnames[millidx])
