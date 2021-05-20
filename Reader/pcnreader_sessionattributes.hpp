/*******************************************************************************
* ----- Persistent Systems Pvt. Ltd., Pune ----------- 
* Copyright (c) 2006
* All rights reserved.
* Name        : PCNReader_SessionAttributes.hpp
* Description : 
*******************************************************************************/
#ifndef  __PCN_READER__SESSIONATTR_
#define __PCN_READER__SESSIONATTR_

#pragma once

#include "utils.hpp"

class NetSessionAttributes
{
public:
    IUString m_sCustomQuery;
    IUString m_sSelectDistinct;
    IUString m_sFilterString;
    IINT32 m_nSortedPorts;
    IUString m_sJoinType;
    NetSessionAttributes();
public:
    ~NetSessionAttributes();
};
#endif
