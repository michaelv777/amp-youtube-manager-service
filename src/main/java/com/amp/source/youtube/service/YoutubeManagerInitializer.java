package com.amp.source.youtube.service;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

public class YoutubeManagerInitializer implements Runnable 
{
    
	private static Logger LOG = 
			LoggerFactory.getLogger(YoutubeManagerInitializer.class);
    
    @Autowired
    private ApplicationContext applicationContext;
    
    protected String contextPath;
    
    public YoutubeManagerInitializer(String contextPath) 
    {
        this.contextPath = contextPath;
    }
    
    @Override
    public void run() 
    {
       
        LOG.info("[FBMANAGER-INIT] About to start service initialization process");
        
        try  
        {
        	YoutubeManagerBean managerBean = (YoutubeManagerBean)
        			this.applicationContext.getBean("YoutubeManagerBean");
        	
        	managerBean.start();
        } 
        catch( Exception e )
        {
        	LOG.error(e.getMessage(), e);
        }
    }
}
