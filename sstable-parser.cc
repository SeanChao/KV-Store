#include <filesystem>
#include <fstream>
#include <iostream>

int main(int argc, char *argv[]) {
    std::string file(argv[1]);
    if (!std::filesystem::exists(file))
        std::cout << argv[1] << " doesn't exist," << std::endl;
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
        fs.read(reinterpret_cast<char *>(&key), sizeof(key));
        fs.read(reinterpret_cast<char *>(&len), sizeof(len));
        char *str = new char[len + 1];
        fs.read(str, len);
        str[len] = '\0';
        std::cout << offest << "\t" << key << ": [" << len << "] " << str
                  << std::endl;
    }
    fs.close();
    return 0;
}