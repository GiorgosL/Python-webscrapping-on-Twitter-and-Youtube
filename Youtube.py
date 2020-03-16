import argparse
from googleapiclient.discovery import build
from apiclient.errors import HttpError
import time

#credential authentication
DEVELOPER_KEY = ''
YOUTUBE_API_SERVICE_NAME = ''
YOUTUBE_API_VERSION = ''
YOUTUBE_COMMENT_URL = ''


class Video(object):
    #video class for metadata setting 
    def __init__(self, video_id='', title='', author='', comments=[], likes=0, lat=0.0, lon=0.0):
        self.__video_id = video_id
        self.__title = title
        self.__author = author
        self.__comments = comments
        self.__likes = likes
        self.__lat = lat
        self.__lon = lon

    def log(self):
        print('Video Id: ' + self.__video_id + '\n' +
              'Video Author: ' + self.__author + '\n' +
              'Video Title: ' + self.__title + '\n' +
              'Video Comments: ' + ' | '.join(map(str, self.__comments)) + '\n' +
              'Video Likes: ' + str(self.__likes) + '\n' +
              'Lat: ' + str(self.__lat) + '\n' +
              'Lon: ' + str(self.__lon) + '\n' +
              '*'*20)
#properties to collect metdata
    @property
    def video_id(self):
        return self.__video_id
    
    @property
    def title(self):
        return self.__title
    
    @property
    def author(self):
        return self.__author

    @property
    def comments(self):
        return  ' | '.join(map(str, self.__comments))

    @property
    def likes(self):
        return self.__likes
    
    @property
    def lat(self):
        return self.__lat
    
    @property
    def lon(self):
        return self.__lon
    

class VideoCrawler(Video):
    def __init__(self):
        pass
#connect to youtube api
    def connect(self):
        return build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=DEVELOPER_KEY)

    def youtube_search(self, options, youtube):
        
        ##Call the search.list method to retrieve results matching the specified
        ##query term.
        search_response = youtube.search().list(
            q=options.q,
            type='video',
            location=options.location,
            locationRadius=options.location_radius,
            part='id,snippet',
            maxResults=options.max_results
        ).execute()

        # Get set(list) of videos  
        search_videos = {search_result['id']['videoId'] for search_result in search_response.get('items', [])}
        video_ids = ','.join(map(str, search_videos))  # concatenate video_ids

        return video_ids

    def get_youtube_videos(self, youtube, video_ids):
        #Call the videos.list method to retrieve location details for each video.
        video_response = youtube.videos().list(
            id=video_ids,
            part='snippet, recordingDetails,statistics'
        ).execute()
        videos = list()
        # Add each result to the list, and then display the list of matching videos.
        for video_result in video_response.get('items', []):
            video = Video(video_result['id'], video_result['snippet']['title'], video_result['snippet']['channelTitle'],
                          self.get_comment_threads(youtube, video_result['id']), video_result['statistics']['likeCount'],
                          video_result['recordingDetails']['location']['latitude'],
                          video_result['recordingDetails']['location']['longitude'])
            
            videos.append(video)

        return videos

    def get_comment_threads(self, youtube, video_id):
        #Call commentThreads method to retrievelist of comments for a specific video_id.

        results = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            textFormat="plainText"
        ).execute()

        comments = [self.remove_non_ascii(item["snippet"]["topLevelComment"]["snippet"]["textDisplay"]) for item in
                    results["items"]]

        return comments

    def remove_non_ascii(self, text):
        return ''.join([i if ord(i) < 128 else ' ' for i in text])

#argument parser including parameters set 
if __name__ == '__main__':       
    parser = argparse.ArgumentParser(description='Youtube')
    parser.add_argument('--q', help='Search term', default='google')
    parser.add_argument('--location', help='Location', default='55.8584097,-4.2513836')
    parser.add_argument('--location-radius', help='Location radius', default='10km')
    parser.add_argument('--max-results', help='Max results', default=50)
    args = parser.parse_args()

    try:
        start = time.time()
        videoCrawler = VideoCrawler()
        youtube = videoCrawler.connect()
        video_ids = videoCrawler.youtube_search(args, youtube)
        videos = videoCrawler.get_youtube_videos(youtube, video_ids)
#printing the gathered video information
        for video in videos:
            print('Video Id: ' + video.video_id + '\n' +
              'Video Author: ' + video.author + '\n' +
              'Video Title: ' + video.title + '\n' +
              'Video Comments: ' + video.comments + '\n' +
              'Video Likes: ' + str(video.likes) + '\n' +
              'Lat: ' + str(video.lat) + '\n' +
              'Lon: ' + str(video.lon) + '\n' +
              '*'*20)
#print number of videos and time passed
        end = time.time()
        print('Videos fetched: %d' % len(videos))
        print ('Time: %.2f seconds.' % (end-start))
    except HttpError as e:
        print ("An HTTP error %d occurred:\n%s" % (e.resp.status, e.content))
    except Exception as ex:
        print (ex.args)
