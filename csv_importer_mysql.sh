#!/bin/bash

DB_USER="root"
DB_PASS="root"
DB_BASE="db_dest"
DB_HOST="127.0.0.1"
LOG="/var/log/csv_importer_mysql.log"

_mysql_exec() {
    [[ -z "$@" ]] && exit 1
    echo "${@}" | mysql -u${DB_USER} -p"${DB_PASS}" -D${DB_BASE} -h${DB_HOST}
}

_mysql_load_csv() {
    [[ -f "$1" ]] || exit 1
    [[ -n "$2" ]] || exit 1
    [[ -n "$3" ]] || exit 1
    file="$1"
    separator="$2"
    table_name="$3"

    query_load="LOAD DATA INFILE '${file}' INTO TABLE ${table_name} COLUMNS TERMINATED BY '${separator}' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES;"

    _mysql_exec "${query_load}"
}

_get_csv_separator() {
    [[ -f "$1" ]] || return 1
    separators=(';' ',' '\t')
    for s in ${separators[@]}; do
        count=$(xsv headers -d "${s}" "${1}" | wc -l)
        if [[ ${count} -gt 1 ]]; then
            echo "${s}"
            return 0
        fi
    done
    return 1
}

_parse_headers_csv_to_sql() {
    local file="$1"
    local dir="$(dirname "${file}")"
    local table_name="$(echo ${file%%\.*})"
    table_name="${table_name##${dir}/}"
    table_name="$( \
        echo ${table_name} |
            tr  '[:upper:]' '[:lower:]' |
            tr -d '(' |
            tr -d ')' |
            sed 's/-/_/g' |
            sed 's/ //g' \
        )"

    local fields="$( \
        xsv headers -j "${file}" |
            iconv -f ISO-8859-1 -t UTF-8 |
            sed 'y/áÁàÀãÃâÂéÉêÊíÍóÓõÕôÔúÚçÇ/aAaAaAaAeEeEiIoOoOoOuUcC/' |
            sed 's/,/_/g' |
            sed 's/ /_/g' |
            sed 's/-/_/g' |
            sed 's/;/ /g' |
            tr '[[:upper:]]' '[[:lower:]]' |
            xargs \
    )"

    local unique_fields=""
    for f in ${fields}; do
        echo ${unique_fields} | xargs -n 1 | grep -Eq "^${f}_[0-9]$"
        [[ $? -eq 0 ]] && continue
        amount=$(echo ${fields} | xargs -n 1 | grep -o "^${f}$" | wc -l)
        if [[ ${amount} -gt 1 ]]; then
            for ((i=1; i<=${amount}; i++)); do unique_fields="${unique_fields} ${f}_${i}"; done
        else
            unique_fields="${unique_fields} ${f}"
        fi
    done

    local columns=$( \
        echo "${unique_fields}" |
            sed 's/^ //g' |
            sed -r 's/(;)|( )/ VARCHAR(255), /g' |
            sed 's/$/ VARCHAR(255)/g' \
    )
    local query="DROP TABLE IF EXISTS ${table_name}; CREATE TABLE ${table_name} (${columns}) ENGINE = MyISAM;"

    echo "${table_name}"
    _mysql_exec "${query}"
    return $?
}

_main() {
    IFS=$'\n'
    for path_of_file in $(find "${DIR}" -iname "*${EXTENSION}"); do
        grep "${path_of_file}" "${LOG}" | tail -1 | grep -q 'done'
        if [[ "$?" -eq 0 ]]; then
            echo "file already imported: '${path_of_file}'"
            continue
        fi

        IFS=$' '
        if [[ ! -f ${path_of_file} ]]; then
            echo "file ${path_of_file} dont exist"
            exit 1
        fi

        table_name=$(_parse_headers_csv_to_sql "${path_of_file}")
        if [[ "$?" -ne 0 ]]; then
            echo "error on file: '${path_of_file}'" >> "${LOG}"
            exit 1
        fi

        separator="$(_get_csv_separator "${path_of_file}")"

        _mysql_load_csv "${path_of_file}" "${separator}" "${table_name}"
        if [[ "$?" -ne 0 ]]; then
            echo "error on mysql_load_csv file: '${path_of_file}'" >> "${LOG}"
            exit 1
        fi
        echo "done import on mysql_load_csv file: '${path_of_file}'" >> "${LOG}"
    done
}

[[ ${#@} -eq 0 ]] && exit 1

while getopts "d:e:" OPT; do
   case "${OPT}" in
      "d") export DIR="${OPTARG}"; test -d "${DIR}" || exit 1;;
      "e") export EXTENSION="${OPTARG}"; test -n "${EXTENSION}" || exit 1;;
      *) exit;;
   esac
done

_main "$1"
