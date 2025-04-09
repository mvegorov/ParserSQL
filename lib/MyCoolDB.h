#ifndef LABWORK_12_MVEGOROV_MYCOOLDB_H
#define LABWORK_12_MVEGOROV_MYCOOLDB_H

#include <string>
#include <vector>
#include <map>
#include <unordered_set>
#include <iostream>
#include <fstream>

class Exception: public std::exception{
public:
    explicit Exception(const std::string& _text){
        text=_text;
    }
    std::string text;
};

class MyCoolDB {
private:
    struct Table;
    std::map<std::string,Table> data;
    std::string data_source_path;
    bool any_changes{false};
    bool must_be_saved{true};
public:
    MyCoolDB(const std::string& path);
    void save();
    ~MyCoolDB();

    std::string Execute(const std::string& request_string);
};


#endif //LABWORK_12_MVEGOROV_MYCOOLDB_H
