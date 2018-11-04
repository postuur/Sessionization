# -*- coding: utf-8 -*-
"""
Created on Sat Nov  3 12:17:21 2018

@author: Joel

Currently doing with the dataset and not systemtime, so timeouts and whatnot
are not handled correctly. 
"""

import json
from io import StringIO
        
class SessionHandler:
    
    def __init__(self):
        self.current_sessions = {}
        
    def read(self, data):
        data = json.loads(data) 
        event = Event(data["timestamp"],data["event_type"],data["user_id"],data["content_id"])
        session_id = event.user_id +"_"+ event.content_id
        
        if session_id not in self.current_sessions:
            self.current_sessions[session_id] = Session(event.user_id, event.content_id)
        
        self.current_sessions[session_id].addEvent(event)
        self.checkTimeouts(event.timestamp) # A bit clunky to check timeouts every time an event comes in, will not ha
        
    def write(self): # Write all out on request.
        for session_id in self.current_sessions.keys():
            print(self.current_sessions[session_id].serialize())
        
        
    def checkTimeouts(self, timestamp): # Hacky for datasets
        for session_id in self.current_sessions.keys():
            if (timestamp - 60) > self.current_sessions[session_id].last_active:
                self.current_sessions[session_id].handleEnd(self.current_sessions[session_id].last_active + 60)
                
    def stop(self):
        for session_id in self.current_sessions.keys():
            if self.current_sessions[session_id].session_end < 0: # Finalize open sessions
                self.current_sessions[session_id].handleEnd(self.current_sessions[session_id].last_active)

class Session:
    
    def __init__(self, user_id, content_id):
        self.user_id = user_id
        self.content_id = content_id
        self.session_start = -1
        self.session_end = -1
        self.total_time = -1
        self.track_playtime = 0
        self.event_count = 0
        self.ad_count = 0
        
        # Helpers
        self.track_start = 0
        self.track_end = 0
        self.last_active = 0
    
    def addEvent(self, event):
        self.event_count += 1
        self.last_active += event.timestamp - self.last_active
        
        if self.session_start == -1: # Regardless of stream_start, is now valid for broken cases
            self.session_start = event.timestamp
        
        if event.event_type == "ad_end": # Can be changed to ad_start if we want partials to count too. Now it counts broken cases 
            self.ad_count += 1
        
        if event.event_type == "track_start": # Now assuming no multiple tracks during session
            self.track_start = event.timestamp
            self.track_end = event.timestamp
        
        if event.event_type == "track_hearbeat": #THERE'S A TYPO IN THE DATA... hearbeat, not heartbeat
            if self.track_start == 0:
                self.track_start = event.timestamp - 10
                self.track_end = event.timestamp -10
            self.track_end += 10
            self.track_playtime = self.track_end - self.track_start
            
        if event.event_type == "track_end":
            self.track_end = event.timestamp
            self.track_playtime = self.track_end - self.track_start
            
        if event.event_type == "stream_end": # Would be prettier to call system-time
            self.handleEnd(event.timestamp)

    def handleEnd(self, timestamp):
        self.session_end = timestamp
        self.last_active += timestamp - self.last_active
        self.total_time = self.session_end - self.session_start
        
    def serialize(self):
        return {
            'user_id' : self.user_id,
            'content_id' : self.content_id,
            'session_start' : self.session_start,
            'session_end' : self.session_end,
            'total_time' : self.total_time,
            'track_playtime' : self.track_playtime,
            'event_count' : self.event_count,
            'ad_count' : self.ad_count,
            }
        
class Event:
    
    def __init__(self, timestamp, event_type, user_id, content_id):
        self.timestamp = timestamp
        self.event_type = event_type
        self.user_id = user_id
        self.content_id = content_id

#---------------------

def callSessions(datafile):
    data = open(datafile).readlines()
    
    sessionhandler = SessionHandler()
    
    #io = StringIO(data) 
    #json.load(io)

    for item in data:
        sessionhandler.read(item)            
        #Note that there should be a system based on current system time, but now this comes from a dataset
    sessionhandler.stop()
    #print("The data:")
    sessionhandler.write()

if __name__ == "__main__":
    datafile = "dataset2.json"
    #print("Going to sessions:")
    callSessions(datafile)
 
