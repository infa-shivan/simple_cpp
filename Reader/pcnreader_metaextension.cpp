/*******************************************************************************
* ----- Persistent Systems Pvt. Ltd., Pune ----------- 
* Copyright (c) 2006
* All rights reserved.
* Name        : PCNReader_MetaExtension.cpp
* Description : 
*******************************************************************************/

#include "pcnreader_metaextension.hpp"

NetMetaExtension::NetMetaExtension(void)
{
    m_sCustomQuery="";
    m_sSelectDistinct="";
    m_sFilterString="";
    m_nSortedPorts=0;
    m_sJoinType="";
}

NetMetaExtension::~NetMetaExtension(void)
{
}
