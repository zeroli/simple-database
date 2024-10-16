#pragma once

#include <string>

namespace strutil {
bool startsWith(const std::string& s, const std::string& t)
{
    return s.find(t) == 0;
}

}  // namespace strutil
