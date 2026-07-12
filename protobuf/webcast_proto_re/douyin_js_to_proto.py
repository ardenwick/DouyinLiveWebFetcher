#!/usr/bin/env python3
'''
# 2026-07-12
rm -rf transport-schema-im.*.js live-schema-im.*.js
wget -c https://lf-webcast-platform.bytetos.com/obj/webcast-platform-cdn/webcast/douyin_live/chunks/transport-schema-im.63ff9a29.js
wget -c https://lf-webcast-platform.bytetos.com/obj/webcast-platform-cdn/webcast/douyin_live/chunks/live-schema-im.aa08852d.js

python douyin_js_to_proto.py transport-schema-im.*.js live-schema-im.*.js

# time protoc -I . --python_betterproto_out=. douyin.proto |& head
# real    5m21.502s
# usr     0m0.107s
# sys     0m0.092s
# See https://github.com/danielgtaylor/python-betterproto/issues/682

rm -rf ../douyin
command mv -f douyin/ ../
command mv -f douyin.proto ../douyin.proto
'''
# See Also
# https://github.com/scx567888/live-room-watcher/blob/master/src/main/proto/douyin_hack/webcast/

import collections
from dataclasses import dataclass, field
from math import ceil
from time import time
import re
import sys
from typing import Dict, List

PROTO_PACKAGE_NAME = 'douyin'

SCALAR_TYPES = {
    "int32": "int32",
    "int64": "int64",
    "int64String": "int64",
    "uint32": "uint32",
    "uint64": "uint64",
    "uint64String": "uint64",
    # https://protobuf.dev/programming-guides/proto3/#scalar
    # sint32
    # sint64
    # fixed32
    # fixed64
    # sfixed32
    # sfixed64
    "float": "float",
    "double": "double",
    "bool": "bool",
    "string": "string",
    "bytes": "bytes",
}


def js_type_to_proto(t: str, quolified=True):
    """
    js 类型 -> proto 类型

    r.webcast.data.PreviewExposeData.Meta.Host.decode
        -> PreviewExposeData.Meta.Host
    e.int32
        -> int32
    """
    if t.startswith('map<') and t.endswith('>'):  # map<string,string>
        key_type, value_type = t[4:-1].split(',', 1)
        return 'map<' + js_type_to_proto(key_type) + ',' + js_type_to_proto(value_type) + '>'

    parts = t.split(".")
    if parts[-1] in SCALAR_TYPES:
        return SCALAR_TYPES[parts[-1]]
    if parts[-1] != "decode":
        return t
    parts = parts[:-1]
    # 去掉: r.webcast.im
    if parts[1] == 'webcast':
        parts = parts[3:]
    return ".".join(parts) if quolified else parts[-1]


@dataclass
class Oneof:
    name: str = ""
    fields: list = field(default_factory=list)


class Message:
    Oneof = collections.namedtuple('Oneof', ['name', 'fields'])

    def __init__(self, full_name: str, is_enum=False):
        self.full_name = full_name
        self.name = full_name.split(".")[-1]
        self.parent = (
            full_name.rsplit(".", 1)[0]
            if "." in full_name
            else None
        )
        self.is_enum = is_enum
        self.fields = []
        self.oneofs : List['Oneof'] = []
        self.children = []


def parse(text: str):
    messages: Dict[str, 'Message'] = {}
    stack = []
    msg = None
    for line in text.splitlines():
        indent = len(line) - len(line.lstrip())
        # message name
        #     e.ChatMessage
        m = re.match(r"\s*\w\.([\w$]+)", line)
        if m:
            name = m.group(1)
            while stack and indent <= stack[-1][0]:
                stack.pop()
            if stack:
                full_name = stack[-1][1] + "." + name
            else:
                full_name = name

            msg = Message(full_name, is_enum=' enum ' in line)
            messages[full_name] = msg

            if msg.parent and msg.parent in messages:
                messages[msg.parent].children.append(full_name)

            if msg.is_enum:
                line = line[line.index(' enum ') + 6:]
                for name, value in re.findall(r"(\w+) = (\d+),", line):
                    msg.fields.append({
                        "name": name,
                        "number": int(value),
                    })

            stack.append((indent, full_name))
            continue

        if msg is None:
            continue

        # message fields
        #     1: ["user_name", e.string, 0]
        #     2: ["alternative_effect_config", map<e.uint32,r.webcast.data.MemberMessage.EffectConfig>, 0]
        m = re.search(
            r'([0-9e]+):\["([^"]+)",(.+),([0-9e]+)\]', line.replace(' ', ''))
        if m:
            msg.fields.append({
                "number": int(float(m.group(1))),
                "name": m.group(2),
                "full_type": js_type_to_proto(m.group(3)),
                "rule": int(m.group(4)),
            })
            continue

        # oneof fields
        words = line.split()
        if words[0] == 'oneof':
            msg.oneofs.append(
                Oneof(name=words[1], fields=words[2:])
            )
            continue

    return messages


def resolve_field_type(field, msg: Message):
    t: str = field["full_type"]
    # repeated
    prefix = "repeated " if (field["rule"] == 3) else ""
    # nested message
    if "." in t:
        if t.startswith(msg.full_name + "."):
            t = t.split('.')[-1]
        # rare case where full type name of the field collides with message name
        elif msg.name == t.split('.')[0]:
            t = PROTO_PACKAGE_NAME + '.' + t
    return prefix + t


def emit_message(messages: List['Message'], name: str, level=0):
    msg: Message = messages[name]
    pad = "  " * level
    out = []
    if msg.is_enum:
        out.append(pad + 'enum ' + msg.name + ' {')
        for f in sorted(msg.fields, key=lambda m: m["number"]):
            out.append(f"{pad}  {f['name']} = {f['number']};")
    else:
        out.append(pad + 'message ' + msg.name + ' {')
        all_oneof_fields = [x for o in msg.oneofs for x in o.fields]
        # fields
        fields = [f for f in msg.fields if f['name'] not in all_oneof_fields]
        for f in sorted(fields, key=lambda m: m["number"]):
            typ = resolve_field_type(f, msg)
            out.append(f"{pad}  {typ} {f['name']} = {f['number']};")

        # oneof fields
        for o in msg.oneofs:
            out.append(f"{pad}  oneof {o.name} {{")
            fields = [f for f in msg.fields if f['name'] in o.fields]
            for f in sorted(fields, key=lambda m: m["number"]):
                typ = resolve_field_type(f, msg)
                out.append(f"{pad}    {typ} {f['name']} = {f['number']};")
            out.append(f"{pad}  }}")

    # nested messages
    for child in msg.children:
        out.append("")
        out.extend(emit_message(messages, child, level + 1))

    out.append(pad + "}")
    return out


def generate_proto(text):
    write_stderr("Generating proto file.\n")

    messages = parse(text)
    roots = [m for m in messages if "." not in m]

    out = [
        'syntax = "proto3";',
        f'package {PROTO_PACKAGE_NAME};',
        '',
    ]
    for root in sorted(roots):
        out.extend(emit_message(messages, root))
        out.append("")
    return "\n".join(out)


def read_file(file_path: str) -> str:
    with open(file_path, 'r', encoding="utf8") as f:
        return f.read()


def write_file(file_path: str, text: str) -> None:
    with open(file_path, 'w', encoding='utf8') as f:
        f.write(text)


def write_stderr(text: str):
    sys.stderr.write(text)
    sys.stderr.flush()


def format_javascript(text: str) -> str:
    start_time = time()
    if len(text) > 1024*1024:
        write_stderr(
            f"Formatting JavaScript text of length {int(len(text)/1024)} KiB, this may take some time.\n")

    import jsbeautifier as jsb
    opts = jsb.BeautifierOptions({
        "indent_size": 4,
        "indent_char": ' ',
        "preserve_newlines": False,
        "end_with_newline": True,
        "max_char": 32768,
        # ["collapse", "expand", "end-expand", "none", "preserve-inline"]
        "brace_style": 'collapse',
        "break_chained_methods": False,
        "space_in_paren": False
    })
    text = text.replace('),', '),\n').replace('},', '},\n')
    out = jsb.beautify(text, opts)

    if time()-start_time > 3:
        write_stderr(f"  -- took {ceil(time() - start_time)}s\n")
    return out


def extract_proto_related_js_snippet(text: str) -> str:
    def extract_number(s: str) -> str:
        # case N:
        m = re.search(r"case\s+([0-9e]+):", s)
        if m:
            return m.group(1)
        # (...)
        m = re.match(r"^[^(]+\((.*)\)[^)]+$", s)
        if not m:
            return ""

        expr = m.group(1)
        expr = re.sub(r"\s", "", expr)
        parts = re.split(r"===?", expr)

        if len(parts) > 0 and re.fullmatch(r"[0-9e]+", parts[0]):
            return parts[0]

        if len(parts) > 1 and re.fullmatch(r"[0-9e]+", parts[1]):
            return parts[1]

        return ""

    result = []
    indent = ""
    last = ""

    text = format_javascript(text)
    write_file(f"formatted.{PROTO_PACKAGE_NAME}.js", text)

    lines = text.splitlines()
    for i, line in enumerate(lines):
        if re.match(
            r'''^\s+((return )?[a-z]\.[A-Z]\w+ = function|[0-9e]+: \[".+|\w\.\w+ === \w\.emptyObject|Object.defineProperty)''',
            line,
        ):  # contains message name, field definition

            # field of map<> type
            if '.emptyObject' in line:
                field = line.split()[0].split('.')[1]
                ftype = 'map<>'

                snippet_lines = lines[i:i+15]
                for j, l in enumerate(snippet_lines):
                    if ' switch ' in l and ' case 1:' in snippet_lines[j+1]:
                        key_type = re.search(
                            '[\w\.]+', snippet_lines[j + 2].split()[2]).group(0)
                        value_type = re.search(
                            '[\w\.]+', snippet_lines[j + 5].split()[2]).group(0)
                        ftype = f"map<{key_type},{value_type}>"
                        break

                result.append(
                    f'''{indent}{extract_number(last)}: ["{field}", {ftype}, 0]''')
            # oneof
            elif ('Object.defineProperty' in line) and ('.oneOfGetter' in lines[i+1]) and ('.oneOfSetter' in lines[i+2]):
                field = line.split('"')[1]
                oneof_fields = re.findall(r'"(\w+)"', lines[i+1])
                result.append(
                    f"{indent}    oneof {field} {' '.join(oneof_fields)}")
            # message name, plain field
            else:
                m = re.match(r"^\s+", line)
                if m:
                    indent = m.group(0)

                line = re.sub(r" = function.*$", "", line)
                line = re.sub(r"return ", "    ", line)
                result.append(line)

            # enum definition
            enum_elements = re.findall(
                r'\w\[\w\[\d+\] = "(\w+)"\] = (\d+)', lines[i+3])

            if ' return ' in lines[i+3] and len(enum_elements) > 0:
                result[-1] += " enum " + " ".join([
                    f"{x[0]} = {x[1]}," for x in enum_elements
                ])

        last = line

    return "\n".join(result)


def compile_proto(file: str):
    from subprocess import run

    write_stderr(f"Compiling proto file '{file}'\n")
    start_time = time()
    run(
        [
            "protoc",
            "-I", ".",
            "--python_betterproto_out=.",
            file,
        ],
        check=False,
    )
    write_stderr(f"  -- took {ceil(time() - start_time)}s\n")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} compiled-proto.js...")
        sys.exit(1)

    text = ''
    for f in sys.argv[1:]:
        text += '\n' + read_file(f)

    text = extract_proto_related_js_snippet(text)

    write_file(PROTO_PACKAGE_NAME + '.snippet.js', text)
    write_file(PROTO_PACKAGE_NAME + '.proto', generate_proto(text))

    compile_proto(PROTO_PACKAGE_NAME + '.proto')
