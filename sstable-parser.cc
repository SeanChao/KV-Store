#include <filesystem>
#include <fstream>
#include <iostream>
// #include <

std::string printTime(const time_t rawtime) {
    struct tm *dt;
    char buffer[30];
    dt = localtime(&rawtime);
    strftime(buffer, sizeof(buffer), "%m/%d %H:%M:%S", dt);
    return std::string(buffer);
}

void readTable(std::string root, int level, int id) {
    std::string file = root + '/' + "level-" + std::to_string(level) +
                       "/sstable-" + std::to_string(id);
    if (level == -1) file = root + '/' + "tmp/sstable-" + std::to_string(id);
    if (!std::filesystem::exists(file)) {
        std::cout << file << " doesn't exist" << std::endl;
        return;
    }
    std::ifstream fs(file, std::ios::binary);
    fs.seekg(-8, std::ios::end);
    uint64_t index = 0;
    fs.read(reinterpret_cast<char *>(&index), sizeof(index));
    fs.seekg(index);
    std::basic_istream<char, std::char_traits<char>>::pos_type indexTablePos =
        fs.tellg();
    fs.seekg(std::ios::beg);
    std::cout << "[meta] indexTable @" << indexTablePos << std::endl;
    while (fs.tellg() < indexTablePos) {
        int offest = fs.tellg();
        uint64_t key = 0;
        uint64_t len = 0;
        time_t time = 0;
        fs.read(reinterpret_cast<char *>(&key), sizeof(key));
        fs.read(reinterpret_cast<char *>(&time), sizeof(time));
        fs.read(reinterpret_cast<char *>(&len), sizeof(len));
        char *str = new char[len + 1];
        fs.read(str, len);
        str[len] = '\0';
        std::cout << "<" << offest << "> " << printTime((time)) << "\t" << key
                  << ": [" << len << "] " << std::string(str, 0, 60)
                  << std::endl;
    }
    fs.close();
}

void readAll(std::string root) {
    // std::string level = "";
    int lv = 0;
    int count = 0;
    std::string level = root + '/' + "level-" + std::to_string(lv);
    while (std::filesystem::exists(level)) {
        std::string filename = level + "/sstable-" + std::to_string(count);
        while (std::filesystem::exists(filename)) {
            std::cout << std::endl << "### " << filename << std::endl;
            // parse the file
            readTable(root, lv, count);
            filename = level + "/sstable-" + std::to_string(++count);
        }
        count = 0;
        level = root + '/' + "level-" + std::to_string(++lv);
    }
    if (std::filesystem::exists(root + "/tmp")) {
        std::string filename =
            root + "/tmp" + "/sstable-" + std::to_string(count);
        while (std::filesystem::exists(filename)) {
            std::cout << std::endl << "### " << filename << std::endl;
            // parse the file
            readTable(root, -1, count);
            filename = root + "/tmp" + "/sstable-" + std::to_string(++count);
        }
    }
}

int main(int argc, char *argv[]) {
    if (argc <= 2) return 1;
    std::string mode(argv[1]);
    std::string root(argv[2]);
    if (mode == "-a")
        readAll(argv[2]);
    else if (mode == "-t") {
        if (argc < 5) {
            std::cout << "Usage -t root lv id" << std::endl;
            return 1;
        }
        int lv = std::atoi(argv[3]);
        int id = std::atoi(argv[4]);
        readTable(root, lv, id);
    }
    return 0;
}