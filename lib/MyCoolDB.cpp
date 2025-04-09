#include "MyCoolDB.h"

const std::unordered_set<std::string> KEYWORDS{"SELECT", "FROM", "WHERE", "JOIN",
                                               "CREATE", "DROP", "UPDATE", "SET", "INSERT", "DELETE"};

const std::unordered_set<std::string> DATA_TYPES{"varchar", "int", "double", "bool"};

const std::unordered_set<char> SEPARATORS{',', '=', '(', ')', '<', '>', '\''};

std::string NULL_VALUE = "NULL";

struct MyCoolDB::Table {
    std::vector<std::string> specifications;
    std::vector<std::string> column_types;
    std::vector<std::string> column_names;
    std::vector<std::vector<std::string>> table;
};

struct RequestTable {
    std::string table_name;
    std::vector<bool> is_column_selected;
    std::vector<bool> is_row_selected;
    std::vector<std::string> column_types;
    std::vector<std::string> column_names;
    std::vector<std::vector<std::string *>> table;
};

bool FillVector(std::ifstream &fin, std::vector<std::string> &vector) {
    std::string line;
    std::getline(fin, line);
    if (line.empty()) {
        return false;
    }

    for (char ch: line) {
        if (ch == 9) { // символ 9 - разделитель столбцов
            vector.push_back("");
        } else {
            vector.back().push_back(ch);
        }
    }

    return true;
}

void PrintVector(std::ofstream &fout, std::vector<std::string> &vector) {
    for (int i = 0; i < vector.size(); ++i) {
        fout << vector[i];
        if (i != vector.size() - 1) {
            fout << (char) 9;
        }
    }
    fout << '\n';
}

int Find(std::vector<std::string> &vector, std::string &str) {
    int j = 0;
    while (j < vector.size()) {
        if (vector[j] == str) {
            break;
        }
        j++;
    }
    if (j == vector.size()) {
        throw (Exception("No such member: " + str));
    }
    return j;
}

std::string DefineType(std::string *str) {
    if (*str == "true" or *str == "false") {
        return "bool";
    }
    int dots = 0;
    for (char ch: *str) {
        if (ch == '.') {
            dots++;
        } else if (!isdigit(ch)) {
            return "varchar";
        }
    }
    if (dots > 1) {
        return "varchar";
    } else if (dots == 1) {
        return "double";
    } else {
        return "int";
    }
}

std::pair<std::string, std::string> Split(std::string &str) {
    std::string first, second;
    int index = 0;
    while (str[index] != '.' and str[index] != '\0') {
        first.push_back(str[index++]);
    }
    if (str[index] == '\0') {
        return {" ", first};
    }
    index++;
    while (str[index] != '.' and str[index] != '\0') {
        second.push_back(str[index++]);
    }
    if (str[index] == '\0') {
        return {first, second};
    } else {
        throw (Exception("No such column: " + str));
    }
}

bool CheckCondition(std::string &left_type, std::string &right_type, std::string *left, std::string &symbol,
                    std::string *right) {
    if (left_type == "NULL") {
        if (right_type == "NULL") {
            return true;
        } else {
            return false;
        }
    } else if (right_type == "NOT NULL") {
        return true;
    } else if (right_type == "NULL") {
        return false;
    }

    if (left_type != right_type and !((left_type == "int" and right_type == "double")
                                      or (left_type == "double" and right_type == "int"))) {
        throw (Exception("WHERE: wrong condition (very different variable types)"));
    }

    if (left_type == "varchar") {
        if (*left == *right and (symbol == "=" or symbol == "IS")) {
            return true;
        } else if (*left > *right and symbol == ">") {
            return true;
        } else if (*left < *right and symbol == "<") {
            return true;
        }
    } else if (left_type == "bool") {
        if (!(symbol == "=" or symbol == "IS")) {
            throw (Exception("WHERE: < and > not supported for boolean variables"));
        }
        if (*left == *right) {
            return true;
        }
    } else {
        if (stod(*left) == stod(*right) and (symbol == "=" or symbol == "IS")) {
            return true;
        } else if (stod(*left) > stod(*right) and symbol == ">") {
            return true;
        } else if (stod(*left) < stod(*right) and symbol == "<") {
            return true;
        }
    }
    return false;
}

MyCoolDB::MyCoolDB(const std::string &path) {
    data_source_path = path;
    std::ifstream fin(path);
    if (!fin.is_open()) {
        throw Exception("Can't open file");
    }

    std::string table_name;
    while (std::getline(fin, table_name)) {
        data[table_name].specifications = {""};
        data[table_name].column_types = {""};
        data[table_name].column_names = {""};
        FillVector(fin, data[table_name].specifications);
        FillVector(fin, data[table_name].column_types);
        FillVector(fin, data[table_name].column_names);
        data[table_name].table.push_back({""});
        while (FillVector(fin, data[table_name].table.back())) {
            data[table_name].table.push_back({""});
        }
        data[table_name].table.pop_back();
    }
}

void MyCoolDB::save() {
    if (!any_changes or !must_be_saved) {
        return;
    }
    std::ofstream fout(data_source_path);
    if (fout.is_open()) {
        for (auto &it: data) {
            fout << it.first << '\n';
            PrintVector(fout, it.second.specifications);
            PrintVector(fout, it.second.column_types);
            PrintVector(fout, it.second.column_names);
            for (auto &vector: it.second.table) {
                PrintVector(fout, vector);
            }
            fout << '\n';
        }
    } else {
        std::cerr << "Can't find source file to save changes";
    }
}

MyCoolDB::~MyCoolDB() {}


void SplitByKeywords(std::map<std::string, std::vector<std::string>> &map, const std::string &string) {
    if (string.back() != ';') {
        throw (Exception("missed \';\'"));
    }
    std::string current_keyword;
    uint32_t index = 0;
    while (string[index] != ';') {
        std::string word;
        while (string[index] == ' ' or string[index] == '\n') {
            index++;
        }
        if (string[index] == ';') {
            break;
        }

        if (string[index] == '\'') {
            word = "\'";
            if (current_keyword.empty()) {
                break;
            }
            map[current_keyword].push_back("\'");
            map[current_keyword].push_back("");
            index++;
            while (string[index] != '\'' and string[index] != '\0') {
                map[current_keyword].back().push_back(string[index++]);
            }
            if (string[index] == '\0') {
                throw (Exception("Second \' missed"));
            }
            index++;
        } else {
            while (string[index] != ' ' and string[index] != '\n' and string[index] != ';') {
                if (SEPARATORS.find(string[index]) != SEPARATORS.end()) {
                    if (word.empty()) {
                        word.push_back(string[index++]);
                    }
                    break;
                }
                word.push_back(string[index++]);
            }
        }

        if ((current_keyword.empty() or map[current_keyword].empty() or map[current_keyword].back() != ",")
            and KEYWORDS.find(word) != KEYWORDS.end()) {

            if (map.find(word) != map.end()) {
                throw (Exception("Multiple usage of keyword: " + word));
            }
            if (word == "JOIN" and !current_keyword.empty() and !map[current_keyword].empty()) {
                if (map[current_keyword].back() == "LEFT" or map[current_keyword].back() == "INNER"
                    or map[current_keyword].back() == "RIGHT") {
                    map["JOIN"] = {" " + map[current_keyword].back()};
                    map[current_keyword].pop_back();
                    current_keyword = "JOIN";
                }
            } else {
                current_keyword = word;
                map[current_keyword] = {};
            }
        } else {
            map[current_keyword].push_back(word);
        }
    }
}

std::string TableToPrettyString(RequestTable &table) {
    std::string result = "\n";
    std::vector<int> max_widths(table.column_names.size());
    for (int j = 0; j < max_widths.size(); ++j) {
        if (!table.is_column_selected[j]) {
            continue;
        }
        max_widths[j] = table.column_names[j].size();
        for (int i = 0; i < table.table.size(); ++i) {
            if (!table.is_row_selected[i]) {
                continue;
            }
            if (table.table[i][j]->size() > max_widths[j]) {
                max_widths[j] = table.table[i][j]->size();
            }
        }
    }
    for (int j = 0; j < table.column_names.size(); ++j) {
        if (table.is_column_selected[j]) {
            for (auto ch: table.column_names[j]) {
                result.push_back(ch);
            }
            for (int k = 0; k < 4 + max_widths[j] - table.column_names[j].size(); ++k) {
                result.push_back(' ');
            }
        }
    }
    while (result.back() == ' ') {
        result.pop_back();
    }

    result.push_back('\n');
    int rows_to_print = 0;
    for (int i = 0; i < table.table.size(); ++i) {
        if (!table.is_row_selected[i]) {
            continue;
        }
        rows_to_print++;
        for (int j = 0; j < table.table[i].size(); ++j) {
            if (!table.is_column_selected[j]) {
                continue;
            }
            for (auto ch: *table.table[i][j]) {
                result.push_back(ch);
            }
            for (int k = 0; k < 4 + max_widths[j] - (*table.table[i][j]).size(); ++k) {
                result.push_back(' ');
            }
        }
        while (result.back() == ' ') {
            result.pop_back();
        }
        result.push_back('\n');
    }
    if (rows_to_print == 0) {
        if (table.column_names.empty()) {
            throw Exception("Syntax error");
        }

        return "No result";
    }

    return result;
}

bool SolveBooleanExpression(std::vector<std::string> &expression, int &index) {
    std::vector<std::string> local_expression;
    while (index < expression.size() and expression[index] != ")") {
        if (expression[index] == "(") {
            if (SolveBooleanExpression(expression, ++index)) {
                local_expression.push_back("1");
            } else {
                local_expression.push_back("0");
            }
            if (expression[index] != ")") {
                throw (Exception("Wrong bracket sequence"));
            }
        } else {
            if (index + 1 != expression.size() and expression[index] == "NOT" and expression[index + 1] == "NOT") {
                index++;
            } else {
                local_expression.push_back(expression[index]);
            }
        }
        index++;
    }
    std::vector<std::string> simplified_expression;

    // NOT
    for (int i = 0; i < local_expression.size(); ++i) {
        if (local_expression[i] == "NOT") {
            if (i + 1 == local_expression.size() or local_expression[i + 1] != "0" and local_expression[i + 1] != "1") {
                throw (Exception("NOT: Syntax error"));
            }
            if (local_expression[i + 1] == "0") {
                simplified_expression.push_back("1");
            } else {
                simplified_expression.push_back("0");
            }
            i++;
        } else {
            simplified_expression.push_back(local_expression[i]);
        }
    }
    local_expression = std::vector<std::string>();
    std::swap(local_expression, simplified_expression);

    // AND
    for (int i = 0; i < local_expression.size(); ++i) {
        if (local_expression[i] == "AND") {
            if (i + 1 == local_expression.size() or simplified_expression.empty() or local_expression[i + 1] != "0" and
                local_expression[i + 1] != "1") {
                throw (Exception("AND: Syntax error"));
            }
            if (simplified_expression.back() == "1" and local_expression[i + 1] == "1") {
                simplified_expression.back() = "1";
            } else {
                simplified_expression.back() = "0";
            }
            i++;
        } else {
            simplified_expression.push_back(local_expression[i]);
        }
    }
    local_expression = std::vector<std::string>();
    std::swap(local_expression, simplified_expression);

    // OR
    for (int i = 0; i < local_expression.size(); ++i) {
        if (local_expression[i] == "OR") {
            if (i + 1 == local_expression.size() or simplified_expression.empty() or local_expression[i + 1] != "0" and
                local_expression[i + 1] != "1") {
                throw (Exception("OR: Syntax error"));
            }
            if (simplified_expression.back() == "1" or local_expression[i + 1] == "1") {
                simplified_expression.back() = "1";
            } else {
                simplified_expression.back() = "0";
            }
            i++;
        } else {
            simplified_expression.push_back(local_expression[i]);
        }
    }

    if (simplified_expression.size() != 1) {
        throw (Exception("Wrong expression"));
    }
    if (simplified_expression[0] == "1") {
        return true;
    } else if (simplified_expression[0] == "0") {
        return false;
    } else {
        throw (Exception("WTF is this -> " + simplified_expression[0]));
    }
}

std::string MyCoolDB::Request(const std::string &request_string) {
    std::map<std::string, std::vector<std::string>> request_map;

    SplitByKeywords(request_map, request_string);

    RequestTable response;

    // FROM
    if (request_map.find("FROM") != request_map.end()) {
        if (request_map.find("SELECT") == request_map.end() and request_map.find("DELETE") == request_map.end()) {
            throw (Exception("FROM: syntax error"));
        }
        if (request_map["FROM"].size() != 1) {
            throw (Exception("FROM: Must be one argument"));
        }
        response.table_name = request_map["FROM"][0];
        if (data.find(response.table_name) == data.end()) {
            throw (Exception("FROM: No such table"));
        }
        response.column_types = data[response.table_name].column_types;
        response.column_names = data[response.table_name].column_names;
        for (auto &vector: data[response.table_name].table) {
            response.table.push_back({});
            for (auto &i: vector) {
                response.table.back().push_back(&i);
            }
        }
        response.is_column_selected = std::vector<bool>(response.column_names.size(), false);
        response.is_row_selected = std::vector<bool>(response.table.size(), true);
    }

    // SELECT
    if (request_map.find("SELECT") != request_map.end()) {
        if (request_map.find("FROM") == request_map.end()) {
            throw (Exception("SELECT: syntax error (FROM required)"));
        }
        if (request_map["SELECT"].empty()) {
            throw (Exception("Must be at least one argument for SELECT"));
        }
        if (request_map["SELECT"][0] == "*" and request_map["SELECT"].size() == 1) {
            response.is_column_selected = std::vector<bool>(response.column_types.size(), true);
        } else {
            if (request_map["SELECT"].size() % 2 == 0) {
                throw (Exception("SELECT: Syntax error"));
            }
            response.is_column_selected = std::vector<bool>(response.column_types.size(), false);
            for (int k = 0; k < request_map["SELECT"].size(); ++k) {
                if (k % 2 == 1) {
                    if (request_map["SELECT"][k] != ",") {
                        throw (Exception("SELECT: Wrong punctuation"));
                    }
                    continue;
                }
                std::pair<std::string, std::string> split = Split(request_map["SELECT"][k]);
                if (split.first == " " or split.first == response.table_name) {
                    int j = Find(response.column_names, split.second);
                    if (response.is_column_selected[j]) {
                        throw (Exception("SELECT: Column already selected"));
                    }
                    response.is_column_selected[j] = true;
                } else if (request_map.find("JOIN") == request_map.end()) {
                    throw (Exception("SELECT: No such column"));
                }
            }
        }
    }

    // UPDATE
    if (request_map.find("UPDATE") != request_map.end()) {
        if (request_map.find("SET") == request_map.end()) {
            throw (Exception("UPDATE: syntax error (where SET)"));
        }
        if (request_map["UPDATE"].size() != 1) {
            throw (Exception("UPDATE: Must be one argument"));
        }
        response.table_name = request_map["UPDATE"][0];
        if (data.find(response.table_name) == data.end()) {
            throw (Exception("UPDATE: No such table"));
        }
        response.column_types = data[response.table_name].column_types;
        response.column_names = data[response.table_name].column_names;
        for (auto &vector: data[response.table_name].table) {
            response.table.push_back({});
            for (auto &i: vector) {
                response.table.back().push_back(&i);
            }
        }
        response.is_row_selected = std::vector<bool>(response.table.size(), true);
    }

    // WHERE
    if (request_map.find("WHERE") != request_map.end()) {
        if (request_map.find("UPDATE") == request_map.end() and (request_map.find("FROM") == request_map.end())) {
            throw (Exception("WHERE: syntax error"));
        }
        std::vector<std::string> *args = &request_map["WHERE"];
        if ((*args).empty()) {
            throw (Exception("Must be some conditions for WHERE"));
        }

        for (int i = 0; i < response.table.size(); ++i) {
            std::vector<std::string> boolean_expression;
            for (int k = 0; k < (*args).size(); ++k) {
                if (k + 2 < (*args).size() and
                    ((*args)[k + 1] == "<" or (*args)[k + 1] == "=" or (*args)[k + 1] == ">" or
                     (*args)[k + 1] == "IS")) {
                    int j = Find(response.column_names, (*args)[k]);
                    std::string *left_arg = response.table[i][j];
                    std::string left_type = response.column_types[j];
                    std::string comparison = (*args)[k + 1];
                    std::string *right_arg = nullptr;
                    std::string right_type;
                    if (*left_arg == "NULL") {
                        left_type = "NULL";
                    }
                    if ((*args)[k + 2] == "\'") {
                        right_arg = &(*args)[k + 3];
                        right_type = "varchar";
                    } else if ((*args)[k + 1] == "IS" and (*args)[k + 2] == "NULL") {
                        right_type = "NULL";
                    } else if (k + 3 < (*args).size() and (*args)[k + 1] == "IS" and (*args)[k + 2] == "NOT" and
                               (*args)[k + 3] == "NULL") {
                        right_type = "NOT NULL";
                    } else {
                        right_arg = &(*args)[k + 2];
                        right_type = DefineType(right_arg);
                        if (right_type == "varchar") {
                            throw (Exception("WHERE: wrong condition"));
                        }
                    }

                    if (CheckCondition(left_type, right_type, left_arg, comparison, right_arg)) {
                        boolean_expression.push_back("1");
                    } else {
                        boolean_expression.push_back("0");
                    }
                    if (right_type == "varchar") {
                        k += 2;
                    } else if (right_type == "NOT NULL") {
                        k += 1;
                    }
                    k += 2;
                } else {
                    boolean_expression.push_back((*args)[k]);
                }
            }

            try {
                int expression_index = 0;
                response.is_row_selected[i] = SolveBooleanExpression(boolean_expression, expression_index);
                if (expression_index != boolean_expression.size()) {
                    throw (Exception("Wrong bracket sequence"));
                }
            } catch (Exception &ex) {
                ex.text = "WHERE: " + ex.text;
                throw ex;
            }
        }
    }

    //DELETE
    if (request_map.find("DELETE") != request_map.end()) {
        if (request_map["DELETE"].size() != 0) {
            throw (Exception("DELETE: Wrong syntax"));
        }
        int rewrite_index = 0;
        for (int i = 0; i < response.is_row_selected.size(); ++i) {
            if (!response.is_row_selected[i]) {
                data[response.table_name].table[rewrite_index++] = data[response.table_name].table[i];
            }
        }
        for (int i = rewrite_index; i < response.is_row_selected.size(); ++i) {
            any_changes = true;
            data[response.table_name].table.pop_back();
        }

        return "Rows deleted: " +
               std::to_string(response.is_row_selected.size() - data[response.table_name].table.size());
    }

    // SET
    if (request_map.find("SET") != request_map.end()) {
        if (request_map.find("UPDATE") == request_map.end()) {
            throw (Exception("SET: syntax error (where UPDATE)"));
        }
        std::vector<std::string> *args = &request_map["SET"];

        for (int k = 0; k < (*args).size(); ++k) {
            int j = Find(response.column_names, (*args)[k]);
            if (k + 2 >= (*args).size() or (*args)[k + 1] != "=") {
                throw (Exception("SET: Syntax error"));
            }
            std::string left_type = response.column_types[j];
            std::string *right_arg;
            std::string right_type;
            if ((*args)[k + 2] == "\'") {
                right_arg = &(*args)[k + 3];
                right_type = "varchar";
            } else {
                right_arg = &(*args)[k + 2];
                right_type = DefineType(right_arg);
                if (right_type == "varchar") {
                    throw (Exception("SET: Syntax error"));
                }
            }
            if (left_type != right_type and !(left_type == "double" and right_type == "int")) {
                throw (Exception("SET: Different data types"));
            }
            any_changes = true;
            for (int i = 0; i < response.table.size(); ++i) {
                if (response.is_row_selected[i]) {
                    *response.table[i][j] = *right_arg;
                }
            }
            if ((*args)[k + 2] == "\'") {
                k += 2;
            }
            k += 3;
            if (!(k >= (*args).size() or (*args)[k] == ",")) {
                throw (Exception("SET: Syntax error"));
            }
        }
        return "Table updated";
    }

    //JOIN
    if (request_map.find("JOIN") != request_map.end()) {
        if (request_map.find("FROM") == request_map.end() or request_map.find("SELECT") == request_map.end()) {
            throw (Exception("JOIN: syntax error"));
        }
        std::vector<std::string> *args = &request_map["JOIN"];
        int start;
        std::string join_type;
        if ((*args)[0][0] == ' ') {
            start = 1;
            join_type = (*args)[0];
        } else {
            start = 0;
            join_type = " INNER";
        }

        if ((*args).size() - start != 5 or (*args)[start + 1] != "ON" or (*args)[start + 3] != "=") {
            throw (Exception("JOIN: Syntax error"));
        }

        std::string left_table_name = response.table_name;
        std::string right_table_name = (*args)[start];
        if (data.find(right_table_name) == data.end()) {
            throw (Exception("JOIN: No such table " + right_table_name));
        }

        std::pair<std::string, std::string> left_split = Split((*args)[start + 2]);
        std::pair<std::string, std::string> right_split = Split((*args)[start + 4]);
        if (left_split.first != response.table_name or right_split.first != right_table_name) {
            throw (Exception("JOIN: Wrong table names"));
        }
        int left_elem = Find(response.column_names, left_split.second);
        int right_elem = Find(data[right_table_name].column_names, right_split.second);
        if (response.column_types[left_elem] != data[right_table_name].column_types[right_elem]) {
            throw (Exception("JOIN: Different data types"));
        }
        std::vector<int> columns_data_index;
        response = RequestTable{left_table_name, {}, {}, {}, {}, {}};
        // В новой таблице мы будем сразу формировать готовый ответ (is_*_selected и column_types нам уже не интересны)
        for (int k = 0; k < request_map["SELECT"].size(); ++k) {
            if (k % 2 == 1) {
                continue;
            }
            std::pair<std::string, std::string> split = Split(request_map["SELECT"][k]);
            if (split.first == left_table_name) {
                int left_j = Find(data[left_table_name].column_names, split.second);
                columns_data_index.push_back(
                        -left_j - 1);  // отрицательность означает что колонна принадлежит левой таблице
                response.column_names.push_back(data[left_table_name].column_names[left_j]);
            } else if (split.first == right_table_name) {
                int right_j = Find(data[right_table_name].column_names, split.second);
                columns_data_index.push_back(right_j);
                response.column_names.push_back(data[right_table_name].column_names[right_j]);
            } else if (split.first != " " and split.first != left_table_name) {
                throw (Exception("JOIN: Too much tables"));
            }
        }

        std::vector<bool> is_left_row_selected(data[left_table_name].table.size(), false);
        std::vector<bool> is_right_row_selected(data[right_table_name].table.size(), false);
        for (int left_i = 0; left_i < data[left_table_name].table.size(); ++left_i) {
            for (int right_i = 0; right_i < data[right_table_name].table.size(); ++right_i) {
                if (data[left_table_name].table[left_i][left_elem] ==
                    data[right_table_name].table[right_i][right_elem]) {
                    is_left_row_selected[left_i] = true;
                    is_right_row_selected[right_i] = true;
                    response.table.push_back({});
                    for (int i: columns_data_index) {
                        if (i < 0) {
                            response.table.back().push_back(&data[left_table_name].table[left_i][-i - 1]);
                        } else {
                            response.table.back().push_back(&data[right_table_name].table[right_i][i]);
                        }
                    }
                }
            }
        }

        if ((*args)[0] == " LEFT") {
            for (int left_i = 0; left_i < data[left_table_name].table.size(); ++left_i) {
                if (!is_left_row_selected[left_i]) {
                    response.table.push_back({});
                    for (int i: columns_data_index) {
                        if (i < 0) {
                            response.table.back().push_back(&data[left_table_name].table[left_i][-i - 1]);
                        } else {
                            response.table.back().push_back(&NULL_VALUE);
                        }
                    }
                }
            }
        } else if ((*args)[0] == " RIGHT") {
            for (int right_i = 0; right_i < data[right_table_name].table.size(); ++right_i) {
                if (!is_right_row_selected[right_i]) {
                    response.table.push_back({});
                    for (int i: columns_data_index) {
                        if (i < 0) {
                            response.table.back().push_back(&NULL_VALUE);
                        } else {
                            response.table.back().push_back(&data[right_table_name].table[right_i][i]);
                        }
                    }
                }
            }
        }

        response.is_column_selected = std::vector<bool>(response.column_names.size(), true);
        response.is_row_selected = std::vector<bool>(response.table.size(), true);
    }

    // CREATE TABLE
    if (request_map.find("CREATE") != request_map.end()) {
        std::vector<std::string> *args = &request_map["CREATE"];
        if ((*args).size() < 6 or (*args)[0] != "TABLE" or (*args)[2] != "(") {
            throw (Exception("CREATE: Wrong syntax"));
        }
        std::vector<std::string> _specification;
        std::vector<std::string> _column_types;
        std::vector<std::string> _column_names;
        int index = 3;
        while (true) {
            if (index + 4 < (*args).size() and (*args)[index] == "PRIMARY" and (*args)[index + 1] == "KEY" and
                (*args)[index + 2] == "(" and (*args)[index + 4] == ")") {
                int j = Find(_column_names, (*args)[index + 3]);
                _specification[j] = "PRIMARY";
                index += 5;
            } else if (index + 9 < (*args).size() and (*args)[index] == "FOREIGN" and (*args)[index + 1] == "KEY" and
                       (*args)[index + 2] == "(" and (*args)[index + 4] == ")" and
                       (*args)[index + 5] == "REFERENCES" and (*args)[index + 7] == "(" and (*args)[index + 9] == ")") {
                int j = Find(data[(*args)[index + 6]].column_names, (*args)[index + 8]);
                if (data[(*args)[index + 6]].specifications[j] != "PRIMARY") {
                    throw (Exception("CREATE TABLE: Reference to non-primary key"));
                }
                if ((*args)[index + 6] == (*args)[1]) {
                    throw (Exception("CREATE TABLE: Can't refer to itself"));
                }
                j = Find(_column_names, (*args)[index + 3]);
                _specification[j] = (*args)[index + 6] + "." + (*args)[index + 8];
                index += 10;
            } else if (index + 1 < (*args).size()) {
                _column_names.push_back((*args)[index]);
                _column_types.push_back((*args)[index + 1]);
                if (DATA_TYPES.find((*args)[index + 1]) == DATA_TYPES.end()) {
                    throw (Exception("CREATE: Wrong syntax (data type expected)"));
                }
                if (index + 3 < (*args).size() and (*args)[index + 2] == "PRIMARY" and (*args)[index + 3] == "KEY") {
                    _specification.push_back("PRIMARY");
                    index += 4;
                } else if (index + 3 < (*args).size() and (*args)[index + 2] == "NOT" and
                           (*args)[index + 3] == "NULL") {
                    if (index + 5 < (*args).size() and (*args)[index + 4] == "PRIMARY" and
                        (*args)[index + 5] == "KEY") {
                        _specification.push_back("PRIMARY");
                        index += 6;
                    } else {
                        _specification.push_back("NOT NULL");
                        index += 4;
                    }
                } else if (index + 8 < (*args).size() and (*args)[index + 2] == "FOREIGN" and
                           (*args)[index + 3] == "KEY" and (*args)[index + 4] == "REFERENCES" and
                           (*args)[index + 6] == "(" and (*args)[index + 8] == ")") {
                    if (data.find((*args)[index + 5]) == data.end()) {
                        throw (Exception("CREATE TABLE: No such table to refer: " + (*args)[index + 5]));
                    }
                    int j = Find(data[(*args)[index + 5]].column_names, (*args)[index + 7]);
                    if (data[(*args)[index + 5]].specifications[j] != "PRIMARY") {
                        throw (Exception("CREATE TABLE: Reference to non-primary key"));
                    }
                    if ((*args)[index + 5] == (*args)[1]) {
                        throw (Exception("CREATE TABLE: Can't refer to itself"));
                    }
                    _specification.push_back((*args)[index + 5] + "." + (*args)[index + 7]);
                    index += 9;
                } else {
                    _specification.push_back("");
                    index += 2;
                }
            } else {
                throw (Exception("CREATE: Wrong syntax"));
            }
            if (index < (*args).size() and (*args)[index] == ",") {
                index++;
            } else if (index < (*args).size() and (*args)[index] == ")") {
                break;
            } else {
                throw (Exception("CREATE: Wrong syntax"));
            }
        }
        if (index + 1 != (*args).size()) {
            throw (Exception("CREATE: Wrong syntax"));
        }
        if (data.find((*args)[1]) != data.end()) {
            throw (Exception("CREATE: Table is already exist"));
        }
        any_changes = true;
        data[(*args)[1]] = Table{_specification, _column_types, _column_names, {}};
        return "Table created";
    }

    // DROP TABLE
    if (request_map.find("DROP") != request_map.end()) {
        std::vector<std::string> *args = &request_map["DROP"];
        if ((*args).size() != 2 or (*args)[0] != "TABLE") {
            throw (Exception("DROP: Wrong syntax"));
        }
        if (data.find((*args)[1]) == data.end()) {
            throw (Exception("DROP: Table is not exist"));
        }
        for (auto &it: data) {  // Найдем дочерние таблицы
            for (auto &i: it.second.specifications) {
                if (Split(i).first == (*args)[1]) {
                    throw (Exception("DROP: Firstly, drop all daughter tables"));
                }
            }
        }
        any_changes = true;
        data.erase((*args)[1]);
        return "Table dropped";
    }

    // INSERT
    if (request_map.find("INSERT") != request_map.end()) {
        std::vector<std::string> *args = &request_map["INSERT"];
        if ((*args).size() < 2 or (*args)[0] != "INTO") {
            throw (Exception("INSERT: Wrong syntax"));
        }
        if (data.find((*args)[1]) == data.end()) {
            throw (Exception("INSERT: No such table"));
        }
        std::vector<int> columns_data_index;
        int k = 2;
        if (k < (*args).size() and (*args)[k] == "(") {
            k++;
            while (k < (*args).size() and (*args)[k] != ")") {
                if (k != 3) {
                    if ((*args)[k] != ",") {
                        throw (Exception("INSERT: Wrong syntax (',' expected)"));
                    }
                    k++;
                }
                if (k == (*args).size()) {
                    break;
                }
                int j = Find(data[(*args)[1]].column_names, (*args)[k]);
                columns_data_index.push_back(j);
                ++k;
            }
            if (k == (*args).size()) {
                throw (Exception("INSERT: Wrong syntax"));
            }
            ++k;
        } else {
            for (int j = 0; j < data[(*args)[1]].column_names.size(); ++j) {
                columns_data_index.push_back(j);
            }
        }
        int rows_inserted = 0;
        if (k < (*args).size() and (*args)[k] != "VALUES") {
            throw (Exception("INSERT: Wrong syntax (VALUES expected)"));
        } else if (k < (*args).size()) {  // insert values
            if ((*args)[k + 1] != "(" or k + 2 >= (*args).size() or (*args).back() != ")") {
                throw (Exception("INSERT: Wrong syntax (values expected)"));
            }
            any_changes = true;
            rows_inserted++;
            data[(*args)[1]].table.push_back(std::vector<std::string>(data[(*args)[1]].column_names.size(), "NULL"));
            k += 2;
            int start = k;
            int columns_data_index_index = 0;
            while (k < (*args).size() and (*args)[k] != ")") {
                if (k != start) {
                    if ((*args)[k] != ",") {
                        throw (Exception("INSERT: Wrong syntax (',' expected)"));
                    }
                    k++;
                }
                std::string value_type;
                if (columns_data_index_index == columns_data_index.size()) {
                    throw (Exception("INSERT: Too much values"));
                }
                std::string column_type = data[(*args)[1]].column_types[columns_data_index[columns_data_index_index]];
                if ((*args)[k] == "\'") {
                    value_type = "varchar";
                    k++;
                } else {
                    value_type = DefineType(&(*args)[k]);
                    if (value_type == "varchar") {
                        throw (Exception("INSERT: Wrong value: " + (*args)[k]));
                    }
                }
                if (value_type != column_type and !(value_type == "int" and column_type == "double")) {
                    throw (Exception("INSERT: Very different variable types"));
                }
                data[(*args)[1]].table.back()[columns_data_index[columns_data_index_index++]] = (*args)[k];
                if (value_type == "varchar") {
                    k++;
                }
                k++;
            }
            if (columns_data_index_index != columns_data_index.size()) {
                throw (Exception("INSERT: Few values"));
            }
        } else {  // Insert into select
            if (request_map.find("SELECT") == request_map.end() or request_map.find("FROM") == request_map.end()) {
                throw (Exception("Insert what?"));
            }
            if (request_map.find("JOIN") != request_map.end()) {
                throw (Exception("Insert or join?"));
            }
            std::vector<int> selects_column_indexes;
            if (request_map["SELECT"][0] == "*") {
                for (int j = 0; j < response.column_names.size(); ++j) {
                    selects_column_indexes.push_back(j);
                }
            } else {
                for (int c = 0; c < request_map["SELECT"].size(); ++c) {
                    if (c % 2 == 1) {
                        continue;
                    }
                    std::pair<std::string, std::string> split = Split(request_map["SELECT"][c]);
                    selects_column_indexes.push_back(Find(response.column_names, split.second));
                }
            }
            if (selects_column_indexes.size() != columns_data_index.size()) {
                throw (Exception("INSERT: Different numbers of columns"));
            }
            for (int i = 0; i < response.is_row_selected.size(); ++i) {
                if (response.is_row_selected[i]) {
                    any_changes = true;
                    rows_inserted++;
                    data[(*args)[1]].table.push_back(
                            std::vector<std::string>(data[(*args)[1]].column_names.size(), "NULL"));
                    for (int c = 0; c < selects_column_indexes.size(); ++c) {
                        data[(*args)[1]].table.back()[columns_data_index[c]] = *response.table[i][selects_column_indexes[c]];
                    }
                }
            }
        }

        for (int i = static_cast<int>(data[(*args)[1]].table.size()) - rows_inserted;
             i < data[(*args)[1]].table.size(); ++i) {
            for (int j = 0; j < data[(*args)[1]].column_names.size(); ++j) {
                if (data[(*args)[1]].specifications[j] == "") {
                    continue;
                } else if (data[(*args)[1]].table[i][j] == "NULL") {
                    throw (Exception("INSERT: Column " + data[(*args)[1]].column_names[j] + " can't contain NULL"));
                } else if (data[(*args)[1]].specifications[j] == "NOT NULL") {
                    continue;
                } else if (data[(*args)[1]].specifications[j] == "PRIMARY") {
                    for (int second_i = 0; second_i < i; ++second_i) {
                        if (data[(*args)[1]].table[i][j] == data[(*args)[1]].table[second_i][j]) {
                            throw (Exception("INSERT: Column " + data[(*args)[1]].column_names[j] +
                                             " must contain unique value"));
                        }
                    }
                } else {  // foreign key
                    std::pair<std::string, std::string> split = Split(data[(*args)[1]].specifications[j]);
                    int second_j = Find(data[split.first].column_names, split.second);
                    int second_i = 0;
                    for (; second_i < data[split.first].table.size(); ++second_i) {
                        if (data[(*args)[1]].table[i][j] == data[split.first].table[second_i][second_j]) {
                            break;
                        }
                    }
                    if (second_i == data[split.first].table.size()) {
                        throw (Exception("INSERT: No such value in PRIMARY KEY"));
                    }
                }
            }
        }

        return "Inserted " + std::to_string(rows_inserted) + " row(s)";
    }

    return TableToPrettyString(response);
}
