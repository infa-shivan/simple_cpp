/*******************************************************************************
* ----- Persistent Systems Pvt. Ltd., Pune ----------- 
* Copyright (c) 2006
* All rights reserved.
* Name        : PCNReader_SessionAttributes.cpp
* Description : 
*******************************************************************************/

#include "pcnreader_sessionattributes.hpp"

NetSessionAttributes::NetSessionAttributes()
{
    m_sCustomQuery="";
    m_sSelectDistinct="";
    m_sFilterString="";
    m_nSortedPorts=0;
    m_sJoinType="";
}

NetSessionAttributes::~NetSessionAttributes()
{
}
