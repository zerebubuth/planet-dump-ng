#ifndef DELTA_HPP
#define DELTA_HPP

/*

Taken from the Osmium library

Copyright 2011 Jochen Topf <jochen@topf.org> and others (see README).

GPLv3 or later.

*/

#include <algorithm>

/**
 * This class models a variable that keeps track of the value
 * it was last set to and returns the delta between old and
 * new value from the update() call.
 */
template<typename T>
class Delta {

public:

    Delta() : m_value(0) {
    }

    void clear() {
        m_value = 0;
    }

    T update(T new_value) {
        std::swap(m_value, new_value);
        return m_value - new_value;
    }

private:

    T m_value;

};

#endif // DELTA_HPP

