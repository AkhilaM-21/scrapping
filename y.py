import requests
import csv
import time
import re
import os
import sys
from datetime import datetime
from typing import Optional, List, Dict, Any
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

API_KEY = "AIzaSyB0BmlMTTcZIffxK16bLDFMyUcN7rOVtFM"  
CHANNEL_URLS = [
# "https://www.youtube.com/@teamlokesh4698",
# "https://www.youtube.com/c/AmaravatiVoice",
# "https://www.youtube.com/@wefor_CBN",
# "https://www.youtube.com/@leoentertainmentchannel/",
# "https://www.youtube.com/@tdpwhatsappstatus4135/",
# "https://www.youtube.com/@TDPPunch",
# "https://www.youtube.com/c/CBNFORTHEBETTERNATION",
# "https://youtube.com/@dalapathiln?si=RkGRBf7Voae1gtN5",
# "https://www.youtube.com/@Okeokkadu_cbn",
# "https://www.youtube.com/c/LNFORYUVA",
# "https://www.youtube.com/@Maheshmedia/",
# "https://www.youtube.com/@WildWolfTelugu/about",
# "https://www.youtube.com/c/WeSupportTDP",
# "https://www.youtube.com/@CBNFORPEOPLE/",
# "https://www.youtube.com/@OpenTalkTeam/about",
# "https://www.youtube.com/c/TheLeoNews/",
# "https://youtube.com/@idhicorrect?si=kETdWdH0lo91N-iB",
# "https://www.youtube.com/@massmama3679/",
# "https://www.youtube.com/@meowmeowpilli7/about",
# "https://www.youtube.com/@PopcornMediaofficial/about",
# "https://www.youtube.com/@RocketTeluguNews/a",
# "https://www.youtube.com/@TakeOneMedia99/",
# "https://www.youtube.com/c/TDPActivist",
# "https://www.youtube.com/@ThinkAndhra",
# "https://www.youtube.com/@ThinkTollywood/videos",
# "https://www.youtube.com/@TDP_YOUTH_WARRIORS/",
# "https://www.youtube.com/@tdp_students",
# "https://youtube.com/@andhraism-q7i?si=bqlYHybCWPxuhYZo",
# "https://youtube.com/@tdpat175?si=3bHR1z1GrL3Y8JlP",
# "https://www.youtube.com/c/VoiceofTDP9",
# "https://www.youtube.com/@peopleopinion2024",
# "https://www.youtube.com/@tdpyuva675/videos",
# "https://www.youtube.com/@bigbosscbn2059",
# "https://www.youtube.com/@FusuCk",
# "https://www.youtube.com/@pinkomedia",
# "https://www.youtube.com/@balayyapunch",
# "https://www.youtube.com/@WildWolfDigi",
# "https://www.youtube.com/@JaiiTdp",
# "https://www.youtube.com/@vanaramedia_official",
# "https://www.youtube.com/@vanara_politics",
# "https://www.youtube.com/@vanara_media",
# "https://youtube.com/@GangTdp",
# "https://youtube.com/@sharechey_mawa?si=TXA4-Hq9fe8vHtWE",
# "https://www.youtube.com/@AbbaKamalHassanYT",
# "https://www.youtube.com/@AlladistaTrolls-N",
# "https://www.youtube.com/@wearetdp/featured",
# "https://www.youtube.com/@MemeRaPushpa",
# "https://www.youtube.com/@MemesBandi",
# "https://youtube.com/@sarcasticsadhana?si=scC-PAzkSGK9hQsF",
# "https://youtube.com/@SuperSubbuOfficial",
# "https://youtube.com/@IamwithNCBN",
# "https://youtube.com/@thaggedheley9?si=P6VEKIaT2ZhoUWsi",
# "https://www.youtube.com/@myfirstvoteforcbn",
# "https://www.youtube.com/@Janagalam",
# "https://www.youtube.com/@TDPforpeople",
# "https://www.youtube.com/@taja3046",
# "https://youtube.com/@apnextcm-jo2ee?si=tAu2YsPTEJPQtYYZ",
# "https://youtube.com/@jananadi-2024?si=bSx9zwBCE9Sz9wC1",
# "https://youtube.com/@tdpcommunity.official?si=tZsQyIkLP9HUOOpB",
# "https://www.youtube.com/@appolitics_2024/featured",
# "https://www.youtube.com/channel/UC8VZtBNbrLgrazQF-SrfODA",
# "https://www.youtube.com/@nayapolitics.Official",
# "https://www.youtube.com/@peoplemedia-en9fy/featured",
# "https://www.youtube.com/@yuvagalamofficial/featured",
# "https://www.youtube.com/@cbnfollower9999",
# "https://www.youtube.com/@tdpkutumbam9731",
# "https://www.youtube.com/@CbnTheLegend2024",
# "https://youtube.com/@tdptrends.official?si=272ubF_n5Qt6qFxj",
# "https://www.youtube.com/@tdpat_175",
# "https://youtube.com/@poweroftdp.official?si=dLBh1yZuAK2FpbAb",
# "https://www.youtube.com/@votefortdp2024",
# "https://www.youtube.com/@Telugu_Sena",
# "https://www.youtube.com/@news24.telugu",
# "https://www.youtube.com/@teamlokesh4698",
# "https://www.youtube.com/@thinkandvote.",
# "https://www.youtube.com/@TeluguSena.Officia",
# "https://www.youtube.com/@TeluguSena2024",
# "https://www.youtube.com/@TeamTdp-xd6yf",
# "https://www.youtube.com/@ChaitanyaRadhamTdp",
# "https://www.youtube.com/@TDPYOUTH963",
# "https://youtube.com/@yuvasena_tdp?si=DZ-tE57MiwhudhT1",
# "https://youtube.com/@tillu_trolls?si=-JFgurphZKlcYifU",
# "https://www.youtube.com/@PointBlankTelugu",
# "https://www.youtube.com/@PointBlankTvDigital",
# "https://www.youtube.com/@PillaluRaMeeru3",
# "https://youtube.com/@imwithlokesh?si=YReJr_9xJHIFHGAm",
# "https://youtube.com/@localpolitics-z8d?si=3aeSOhWWltD3NX4R",
# "https://www.youtube.com/@vamsikgottipati",
# "https://youtube.com/@publicvision-n5n?si=hbRKx9tLXvIgigH_",
# "https://www.youtube.com/channel/UCGueOBvBu3N3CuSZDB1jrJg",
# "https://www.youtube.com/@politicalikon",
# "https://youtube.com/@crazykanna0?si=GpINjjsh4FdTnG9D",
# "https://www.youtube.com/channel/UCX0xQCu0wNNPvka6edHNRew",
# "https://youtube.com/@publicpulse-ap?si=gt2tEFknqCQT7m-X",
# "https://youtube.com/@ap_talks..0?si=crLxvz1xKM39yyDq]",
# "https://www.youtube.com/@BANKUSEENU",
# "https://youtube.com/@jaitelugudesam2029?si=xMHwoSNcRmW59Zai",
# "https://youtube.com/@letstalk-telugu?si=JOZvtktBq9ITxHIe",
# "https://youtube.com/@tdpera?feature=shared",
# "https://www.youtube.com/@sivazee/videos",
# "https://www.youtube.com/channel/UCJCsh1Th4Rze_orin4gzkRg",
# "https://www.youtube.com/channel/UC3MpAScuECXdkBp0Oks2ouA",
# "https://www.youtube.com/@tdpstatus",
# "https://www.youtube.com/@Jagan730FakePromises",
# "https://www.youtube.com/@We_For_CBN",
# "https://www.youtube.com/@lokeshforpeople.29",
# "https://youtube.com/@teluguvaradhi.official?si=yzelqJ1cVLsOUAaR",
# "https://www.youtube.com/@RMSNEWS7",
# "https://www.youtube.com/@TDP_LOINS",
# "https://youtube.com/@appolitics.official?si=tfyLBLac6q0CLuOo",
# "https://www.youtube.com/@WildWolfFocus/videos",
# "https://youtube.com/@WildWolfVijayawada?feature=shared",
# "https://www.youtube.com/@WildWolfTVBhumi",
# "https://www.youtube.com/@WildWolfTVHealth",
# "https://www.youtube.com/@WildWolfLife",
# "https://www.youtube.com/@WildWolfTrending/videos",
# "https://www.youtube.com/@SaaguNela",
# "https://www.youtube.com/@AndhrulaAtmagouravam/videos",
# "https://www.youtube.com/@YesVCan/videos",
# "https://www.youtube.com/@Ap_Poitical_Pulse/videos",
# "https://www.youtube.com/@seemaraja557",
# "https://www.youtube.com/@SeemaRaja2.O",
# "https://www.youtube.com/@SEEMARAJASHORTVIDEOS",
# "https://www.youtube.com/@Team_CBN",
# "https://youtube.com/@itscbnmark?si=olkOTsTi2Mne8lWp",
# "https://www.youtube.com/@LeoTodayNews",
# "https://www.youtube.com/@LeoBuzz",
# "https://www.youtube.com/@LeoTelangana",
# "https://youtube.com/@chaitanyaratham1?si=kGiMbzkzccVPrav6",
# "https://www.youtube.com/@Raitunestham",
# "https://youtube.com/@journalistvali?si=6mPutxTr5mBxJFBX",
# "https://www.youtube.com/@Tdpyuvashakthi",
# "https://www.youtube.com/@TrendNaraLokesh",
# "https://www.youtube.com/@CycleSena",
# "https://www.youtube.com/@TrendCBN",
# "https://www.youtube.com/@tdpsena",
# "https://www.youtube.com/@RISEOFTDP",
# "https://www.youtube.com/@metanewstelugu",
# "https://www.youtube.com/@MetaPlustelugu/videos",
# "https://www.youtube.com/c/NaraChandrababuNaiduofficial",
# "https://www.youtube.com/@naralokeshofficial",
# "https://www.youtube.com/c/TeluguDesamPartyOfficial",
# "https://www.youtube.com/@andhrachoice",
# "https://www.youtube.com/@TeamYellowtdp",
# "https://www.youtube.com/@SyeRaaTelugoda",
# "https://youtube.com/@tdpdalam?si=xzK1lT4y9ARG3Pmu",
# "https://www.youtube.com/@ElevenKids6093",
# "https://youtube.com/@justicechowdharyyy?si=__931Myw21QNUSJd",
# "https://www.youtube.com/@voiceofandhrapradesh/",
# "https://www.youtube.com/@journalistreport",
# "https://www.youtube.com/@JanaChaithanyam",
# "https://www.youtube.com/@DrPonguruNarayanaOfficial",
# "https://www.youtube.com/@UstaadTrolls",
# "https://www.youtube.com/@Andhrula_Galam",
# "https://www.youtube.com/@tdpfanboy",
# "https://www.youtube.com/@AaveshamRaja",
# "https://www.youtube.com/@ItheyOk",
# "https://www.youtube.com/@JspYuvashakthi",
# "https://www.youtube.com/@LetsTalkAP",
# "https://www.youtube.com/@PunchPaduddi",
# "https://www.youtube.com/@aphatesjagan",
# "https://www.youtube.com/@PowerTeluguTVChannel",
# "https://www.youtube.com/@FactsAboutAP",
# "https://www.youtube.com/@AnnaNTROfficial",
# "https://www.youtube.com/@TDPSpeakers",
# "https://youtube.com/@Apvoiceofficial?si=zqn1v8ONu4End0f_",
# "https://youtube.com/@tdpfangirl?si=w8HaIsDeg_5GIPLi",
# "https://youtube.com/@anthanthamatrame?si=xLPq5Go9WYZHHcJ4",
# "https://youtube.com/@Santhubabuyellapu7569?si=1gF3s1Owdke7fu8F",
# "https://youtube.com/@PoliticalMoji2.0?feature=shared",
# "https://www.youtube.com/@TheTrendyNewsOfficial",
# "https://www.youtube.com/@TeluguVaradhi-2024",
# "https://www.youtube.com/@tdppalakondaofficial",
# "https://www.youtube.com/@tdpkurupamofficial",
# "https://www.youtube.com/@TdpParvathipuram.official",
# "https://www.youtube.com/@Tdpsalurofficial",
# "https://www.youtube.com/@Tdparakuvalleyofficial",
# "https://www.youtube.com/@tdppaderuOfficial",
# "https://www.youtube.com/@tdprampachodavaramofficial",
# "https://www.youtube.com/@TDPlchchapuramOfficial",
# "https://www.youtube.com/@Tdppalasaofficial",
# "https://www.youtube.com/@Tdptekkaliofficial",
# "https://www.youtube.com/@Tdppathapatnam.official",

# "https://www.youtube.com/@TdpSrikakulamofficial",
# "https://www.youtube.com/@TdpAmadalavalasaofficial",
# "https://www.youtube.com/@TdpNarasannapetaofficial",
# "https://youtube.com/@tdpetcherlaofficial?si=Tlg3ao4kbY2427Al",
# "https://youtube.com/@tdprajamofficial?si=whclKdflvepaKhaj",
# "https://youtube.com/@tdpbobbiliofficial?si=C8jG0L0K8U1rW2wv",
# "https://youtube.com/@tdpcheepurupalliofficial?si=UofhmxCWzsKPLDe8",
# "https://youtube.com/@tdpgajapathinagaram.official?si=6-eqU5vRufIzi1Rh",
# "https://youtube.com/@tdpnellimarla-ue9vf?si=IcKJLffuqmuFKkXL",
# "https://youtube.com/@tdpvizianagaram1982?si=JBLBFsn3kjT7BJwN",
# "https://www.youtube.com/@TDPChodavaramOfficial",
# "https://www.youtube.com/@TDPMadugulaOfficial",
# "https://www.youtube.com/@TDPAnakapalleofficial",
# "https://www.youtube.com/@TDPPendurthiofficial",
# "https://www.youtube.com/@TDPYelamanchiliofficial",
# "https://www.youtube.com/@TDPPayakaraopetaofficial",
# "https://www.youtube.com/@TdpNarsipatnamofficial",
# "https://www.youtube.com/@Tdp.Badvel",
# "https://www.youtube.com/@Tdp.Kadapa",
# "https://www.youtube.com/@TdpPulivendla142",
# "https://youtube.com/@tdpkamalapuramac?si=YOO40RkJGXAMofoJ",
# "https://youtube.com/@tdpjammalamaduguac?si=xk83BhnDUAjoMwFs",
# "https://www.youtube.com/@TDP.Proddatur",
# "https://youtube.com/@tdp.mydukur?si=IvBxEyR02fWAuHqg",
# "https://www.youtube.com/@TDP__Nandikotkur",
# "https://www.youtube.com/@Tdp.panyam",
# "https://www.youtube.com/@Tdp.Nandyal",
# "https://www.youtube.com/@Tdp.Banaganapalle",

# "https://youtube.com/@tdp.kurnool?si=kG5T9UoZpVlBM3_U",
# "https://youtube.com/@tdppattikonda?si=Lavjen8zsE9vaTQ_",
# "https://youtube.com/@tdp.kodumur?si=qyDwnOziyfUtwXcn",
# "https://youtube.com/@tdpyemmiganur-i5g?si=XjAzmwAmzppDoVsA",
# "https://youtube.com/@tdpmantralayam?si=6cH4SDeclb7_Cp_n",
# "https://youtube.com/@tdpadoniofficial?si=vQAka_YOEGjzHlfU",
# "https://youtube.com/@tdpalur?si=dXrrd0-omJnALBom",
# "https://youtube.com/@singanamalatdp?si=bmbwZNKQk8H8agwt",
# "https://youtube.com/@kalyandurgtdp?si=vEeuTFLRs2jV_5gj",
# "https://youtube.com/@rayadurgtdp?si=yrBIiZcI_vayBpqa",
# "https://www.youtube.com/@Tdp.Tadpatri",
# "https://www.youtube.com/@TdpUravakonda",
# "https://youtube.com/@tdpanantapururban?si=G62jab-F1fNWwUx8",
# "https://www.youtube.com/@TDPGUNTAKAL",
# "https://youtube.com/@SudhakarTalks",
# "https://www.youtube.com/@MMTT_2024",
# "https://youtube.com/@pillajagannadham-420?si=SGaf6ZN7vYjX30a3",
# "https://www.youtube.com/channel/UCQqnLaxZp51oeIk7085KdSQ",
# "https://youtube.com/@tdp_connects?si=fscIg1aCKpx1kx-V",
# "https://youtube.com/@telugupatriot-kn?si=4R5h8Dt-DNSKHnUa",
# "https://www.youtube.com/@marolokam7",
# "https://youtube.com/@surthanistudio?si=AkwYV6N4kOZLpxaR",
# "https://youtube.com/@kiraakrp?si=UUqe4DtJfPi_74iZ",
# "https://youtube.com/@wallposter_official?si=seX6h-Kz6QktESVC",
# "https://youtube.com/@isa001?si=MqluecX9pqL-YEJ0",
# "https://youtube.com/@tdptrends?si=EAoj8CoO_idS-GNc",
# "https://www.youtube.com/channel/UCW1B5hR0huuIUKQsw1HbAlA",
# "https://www.youtube.com/channel/UC9nuBez-ClebU7zktWPaXpg",
# "https://youtube.com/@seemaraja557?si=rU4kTNOztNpNi8A4",
# "youtube.com/channel/UCFauz0-tMi0XANULkPsxm0w/",
# "https://youtube.com/@vanara_telugu?si=M_nL1xR68bzAvYUB",
# "https://youtube.com/@vanarabhakti?si=5tE9m_8EepuPezpW",
# "https://youtube.com/@vanara_shorts?si=AS3O2Q49p8Kjiwca",
# "https://youtube.com/@kavya_reports?si=Ze9fSzzj7bulcBQI",
# "https://www.youtube.com/channel/UCLGo-ZxGLnFMWFeUvUs04bg",
# "https://www.youtube.com/@PublicVox-m8b",
# "https://www.youtube.com/@TeluguTrending",
# "https://www.youtube.com/@bharathimedia",
# "https://www.youtube.com/@BharathiTVTelugu",
# "https://www.youtube.com/@TeluguSamajam",
# "https://youtube.com/@politicaltrolls?si=UpK34EClnNGjlUaD",
# "https://studio.youtube.com/channel/UCrF-x3Wfdr3wYrcYA0eoNqg",
# "https://studio.youtube.com/channel/UCaSMGWNV-PU70xq8W5w5BZw",
# "https://www.youtube.com/@TeluguYuvathaOfficial",
# "https://www.youtube.com/@TeluguMahilaOfficial",
# "https://www.youtube.com/@TeluguRaithuOfficial",
# "https://www.youtube.com/@APTNTUCOfficial",
# "https://www.youtube.com/@APTNSFOfficial",
# "https://www.facebook.com/TSNVAPOfficial",
# "https://www.youtube.com/@HelloAPByeByeYCP_/shorts",
# "https://www.youtube.com/@TeluguTV24_/shorts",
# "https://www.youtube.com/channel/UCMxY_NpHsPuoA6fphA84U7w",
# "https://www.youtube.com/channel/UCd4h_MoOoEGF4TTsF2KXjEw",
# "https://www.youtube.com/channel/UC6EyS-RlziC3C_nIQX6IXAA",
# "https://www.youtube.com/channel/UCVgZ1ku8MeDQQnM4kfjaqJQ",
# "https://www.youtube.com/channel/UC5MOlwPyGbeeCoarq1EkSJQ",
# "https://www.youtube.com/channel/UCxuaTMzPXw0RwGxQeCZNN5A",
# "https://www.youtube.com/channel/UCWSsi4xeyYoJ2L9tA7glJQg",
# "https://www.youtube.com/@CBNUpdats-c5g",
# "https://www.youtube.com/@SudhakarShorts007",
# "https://www.youtube.com/@AnalystSudhakar",
# "https://www.youtube.com/@politicaltrolls",
# "https://www.youtube.com/@politicaltrolls",
# "https://www.youtube.com/@DynamicAndhra",
# "https://www.youtube.com/@CBNFORTHEBETTERFUTURE",
# "https://www.youtube.com/@NaraLokeshUpdates",
# "https://www.youtube.com/@CBNFORTHEBETTERFUTURE",
# "https://www.youtube.com/@s5politicsnewstelugu",
# "https://www.youtube.com/@TrendsetterTelugu",
# "https://www.youtube.com/@rajaktalks9",
# "https://www.youtube.com/@rvmc893",
# "https://youtube.com/@DailyLooksOfficial?si=CEQx3-2JeC7vsFwY",
# "https://youtube.com/@sreenitv?feature=shared",
# "https://www.youtube.com/@MSRTV",
# "https://youtube.com/@pointoutnewsexclusive?si=j9iPlUWvACP-MVDD",
# "https://youtube.com/@POINTOUTNEWSTELUGU?si=KlWeHqM7lMOpxEMI",
# "https://youtube.com/@Bestnewstelugu?si=0pd8EwYUgaxudCVH..",
# "https://www.youtube.com/@janavahini-po6eu",
# "https://www.youtube.com/@cinemapower79",
# "https://www.youtube.com/@PrajaAdda",
# "https://www.youtube.com/@ManaChaitanyam-fl2fc",

"https://www.youtube.com/@AADYATVTELUGU",
"https://www.youtube.com/@aadyatalks",
"https://www.youtube.com/@AADYAplus",
"https://www.youtube.com/@ManaTelugu",
"https://www.youtube.com/@MANAVELUGUtelugu",
"https://www.youtube.com/@RedTvNews/videos",
"https://www.youtube.com/@redtvtelugu6943/videos",
"https://www.youtube.com/@FirstTelugu7164/videos",
"https://www.youtube.com/@FirstTelugudigital/videos",
"https://www.youtube.com/@manaandhraofficial/videos",
"https://www.youtube.com/@JoinYuvaGalam/about",
"https://www.youtube.com/@cbnagain4576",
"https://www.youtube.com/@IKMRNews",
"https://youtube.com/@Ra_Kadili_Ra?si=9J-djMeLyOaatnqR",
"https://www.youtube.com/channel/UCTiP2rQ6K_eFovNU-w268uA",
"https://youtube.com/@itdprayachoti",
"https://youtube.com/@itdp..prajakshetram694",
"https://www.youtube.com/@awcnews/videos",
"https://www.youtube.com/@SahithiTv2/videos",
"https://www.youtube.com/@TeluguCinemaBrother",
"https://www.youtube.com/@AlwaysFilmy",
"https://www.youtube.com/@FridayCulture",
"https://www.youtube.com/@AvighnaForYou/videos",
"https://www.youtube.com/@cinemaculture2783",
"https://www.youtube.com/@SimhapuriMedia/videos",
"https://www.youtube.com/@AlwaysPoliticalAdda",
"https://www.youtube.com/@AvighnaTv",
"https://www.youtube.com/c/Newsmarg/videos",
"https://www.youtube.com/channel/UCkndCpVT0EbSRQLv_8XUV7w/videos",
"https://www.youtube.com/channel/UCOCARzQmhZ1Mea8O3LVSAWA",
"https://www.youtube.com/c/SahithiMedia",
"https://www.youtube.com/@nbkculture3202/videos",
"https://www.youtube.com/c/AroundTelugu",
"https://www.youtube.com/@GullyPolitics/videos",
"https://www.youtube.com/channel/UC1zWLJeefjDLIydXoaOPR7A",
"https://www.youtube.com/@SunrayMedia",
"https://www.youtube.com/@TollyHungama",
"https://www.youtube.com/@trendingpoliticsnews",
"https://www.youtube.com/@PrajaDarbarTV",
"https://www.youtube.com/@politicaltrendingtv8080",
"https://www.youtube.com/@politicalbuzztv1203",
"https://www.youtube.com/@YbrantTV",
"https://www.youtube.com/@TeluguPoliticalTrending",
"https://www.youtube.com/@Movieblends",
"https://www.youtube.com/@andhralifetv938",
"https://www.youtube.com/@mixedcinemaculture",
"https://www.youtube.com/@tollyfilms7037",
"https://www.youtube.com/@nrijanasena",
"https://www.youtube.com/@TeluguFilmyYT",
"https://www.youtube.com/@FilmyHook",
"https://www.youtube.com/@TajaFilmy",
"https://www.youtube.com/@megaculture7378",
"https://www.youtube.com/@APPrajaVaradhi-lf2bp",
"https://www.youtube.com/@poweroftdp",
"https://www.youtube.com/@Mana_Times",
# "https://www.youtube.com/@NavyandhraLive",
# "https://www.youtube.com/@Mana_Circle",
# "https://www.youtube.com/@Mixed-Channel",
# "https://www.youtube.com/@manacinematalks",
# "https://www.youtube.com/@JANAVARADHI",
# "https://www.youtube.com/@Mana_Politics_Live",
# "https://www.youtube.com/@Mana_Balam",
# "https://youtube.com/@123Nellore",
# "https://youtube.com/@ntimesnews?feature=shared",
# "https://youtube.com/@channel9hd?si=f65agvV6mJ9dOaQv",
# "https://youtube.com/@NDNNewsLive",
# "https://www.youtube.com/@TV24Studio",
# "https://www.youtube.com/@KavyasMedia",
# "https://www.youtube.com/@harshaguntupallivlogs5373",
# "https://www.youtube.com/@Kasalaahmed",
# "https://www.youtube.com/@pjnewsworld",
# "https://youtube.com/@ettelugunews?si=72byJ2nE9iKnp968",
# "https://www.youtube.com/@NationalistHub",
# "https://www.youtube.com/@NHTV24X7",
# "https://www.youtube.com/@TreeMediaNews",
# "https://www.youtube.com/@suvarnamediaexclusive",
# "https://www.youtube.com/@dialnewsinfo",
# "https://www.youtube.com/@PoliticalLine",
# "https://www.youtube.com/@GharshanaMedia",
# "https://www.youtube.com/@GharshanaMediaBvr",
# "https://www.youtube.com/@News25Channel/",
# "https://www.youtube.com/@TeluguTodayOfficial",
# "https://www.youtube.com/@SasiMedia",
# "https://www.youtube.com/@AndhraVoice",
# "https://www.youtube.com/@lokeshpadayatra",
# "https://www.youtube.com/@GaganaMedia/",
# "https://www.youtube.com/@RajaneethiLive",
# "https://www.youtube.com/@ChetanaMedia",
# "https://www.youtube.com/@MAHASENA-Rajesh",
# "https://www.youtube.com/@varahinews",
# "https://www.youtube.com/@Filmylookslive",
# "https://youtube.com/@DailyLooksOfficial?si=CEQx3-2JeC7vsFwY",
# "https://youtube.com/@sreenitv?feature=shared",
# "https://www.youtube.com/@MSRTV",
# "https://www.youtube.com/@TreeMediaNews",
# "https://www.youtube.com/@suvarnamediaexclusive",
# "https://www.youtube.com/@dialnewsinfo",
# "https://www.youtube.com/@jaganfailedCM",
# "https://www.youtube.com/@PoliticalLine",
# "https://www.youtube.com/@TeluguPopularTV",
# "https://www.youtube.com/@PopularTVChannel",
# "https://www.youtube.com/@telugudaily",
# "https://www.youtube.com/@WallPost/videos",
# "https://youtube.com/@KiranTV2022?feature=shared",
# "https://youtube.com/@KiranTVNews?feature=shared",
# "https://youtube.com/@LifeAndhraOfficial",
# "https://www.youtube.com/@BadudeBadudus/",
# "https://www.youtube.com/@politicalmojitelugu",
# "https://youtube.com/@TdpFighters",
# "https://www.youtube.com/@apneedscbn8558/",
# "https://youtube.com/@Tdptrends",
# "https://www.youtube.com/@ITDPNALLARI",
# "https://www.youtube.com/@nijamAnna/about",
# "https://www.youtube.com/@DHONEITDPSUBBAREDDYANNAYUVASEN",
# "https://www.youtube.com/@aadhyamedia388",
# "https://www.youtube.com/@AvighnaMedia/videos",
# "https://www.youtube.com/@TeluguStates",
# "https://www.youtube.com/@AvighnaPolitical",
# "https://www.youtube.com/channel/UCKU3y2fHgCUBqE6n9fCbNrg/videos",
# "https://www.youtube.com/@TeluguPopularTV",
# "https://www.youtube.com/@PopularTVChannel",
# "http://www.youtube.com/@Vocal_of_Local",
# "https://www.youtube.com/@TEAM_LN",
# "https://www.youtube.com/@YuvaSena2024",
# "https://www.youtube.com/@Santhubabu2.0-rd7dp",
# "https://www.youtube.com/@KirikiriTvchannel",
# "https://www.youtube.com/@BhavishyathukuGuarantee",
# "https://www.youtube.com/@byebyejaganbyebyejagan",
# "https://www.youtube.com/@prajavedika3640",
# "https://www.youtube.com/@psychopovalicycleravali",
# "https://youtube.com/@YuvaGalamln?si=rWk-Imet6D9LMJYJ"
]

API_BASE = "https://www.googleapis.com/youtube/v3"
BATCH_SIZE = 50 
DB_BATCH_SIZE = 500

load_dotenv()
MONGO_URL = os.getenv("ATTENDANCE_MONGO_URL")



logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class YouTubeAPI:
    """YouTube API client with rate limiting and retry logic"""
    
    def __init__(self, api_key: str, base_url: str = API_BASE):
        self.api_key = api_key
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; YouTubeDataCollector/1.0)"
        })
    
    def call_api(self, endpoint: str, params: Dict[str, Any], max_retries: int = 3) -> Dict[str, Any]:
        """Make API call with exponential backoff retry"""
        url = f"{self.base_url}/{endpoint}"
        params = params.copy()
        params["key"] = self.api_key
        
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, params=params, timeout=15)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as e:
                if response.status_code == 403 and 'quotaExceeded' in response.text:
                    logger.error("API quota exceeded")
                    raise
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  
                    logger.warning(f"API error: {e}, retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    raise
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.warning(f"Request error: {e}, retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    raise
        raise RuntimeError(f"API call failed after {max_retries} retries")

class YouTubeChannelProcessor:
    """Process YouTube channel data"""
    
    def __init__(self, api_client: YouTubeAPI):
        self.api = api_client
        self.channel_patterns = [
            r'"channelId"\s*:\s*"([^"]+)"',
            r'<meta itemprop="channelId" content="([^"]+)"',
            r'"externalId"\s*:\s*"([^"]+)"'
        ]
    
    def resolve_channel_id(self, channel_url: str) -> Optional[str]:
        """Resolve channel ID from URL"""
        # Direct channel URL
        if "/channel/" in channel_url:
            return channel_url.split("/channel/")[1].split("/")[0].split("?")[0]
        
        # Try to extract from HTML
        channel_id = self._extract_from_html(channel_url)
        if channel_id:
            return channel_id
        
        # Fallback: search by handle
        return self._search_channel_by_handle(channel_url)
    
    def _extract_from_html(self, url: str) -> Optional[str]:
        """Extract channel ID from HTML page"""
        try:
            response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
            response.raise_for_status()
            html = response.text
            
            for pattern in self.channel_patterns:
                match = re.search(pattern, html, re.IGNORECASE)
                if match:
                    return match.group(1)
        except Exception as e:
            logger.debug(f"Failed to extract from HTML: {e}")
        return None
    
    def _search_channel_by_handle(self, url: str) -> Optional[str]:
        """Search channel by handle"""
        handle = url.rstrip("/").split("/")[-1].replace("@", "")
        
        try:
            data = self.api.call_api("search", {
                "part": "snippet",
                "q": handle,
                "type": "channel",
                "maxResults": 1
            })
            
            if data.get("items"):
                return data["items"][0]["snippet"]["channelId"]
        except requests.exceptions.HTTPError as e:
            # Re-raise quota exceeded errors so they can be handled by the main loop
            if e.response is not None and e.response.status_code == 403 and 'quotaExceeded' in e.response.text:
                raise
        except Exception as e:
            logger.error(f"Failed to search channel: {e}")
        
        return None
    
    def get_uploads_playlist_id(self, channel_id: str) -> str:
        """Get uploads playlist ID for a channel"""
        data = self.api.call_api("channels", {
            "part": "contentDetails",
            "id": channel_id
        })
        
        if not data.get("items"):
            raise ValueError(f"Channel {channel_id} not found")
        
        return data["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    
    def get_channel_info(self, channel_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """Get channel information (snippet, statistics)"""
        channel_info = {}
        
        for i in range(0, len(channel_ids), BATCH_SIZE):
            batch = channel_ids[i:i + BATCH_SIZE]
            
            data = self.api.call_api("channels", {
                "part": "snippet,statistics",
                "id": ",".join(batch)
            })
            
            for item in data.get("items", []):
                cid = item["id"]
                snippet = item.get("snippet", {})
                stats = item.get("statistics", {})
                
                channel_info[cid] = {
                    "channel_created_date": snippet.get("publishedAt"),
                    "channel_title": snippet.get("title"),
                    "total_active_subscribers": self._safe_int(stats.get("subscriberCount"))
                }
            
            time.sleep(0.1) 
        
        return channel_info

class VideoDataProcessor:
    """Process video data"""
    
    @staticmethod
    def fetch_video_ids(playlist_id: str, api_client: YouTubeAPI, max_pages: Optional[int] = None) -> List[str]:
        """Fetch all video IDs from a playlist"""
        video_ids = []
        next_page_token = None
        page_count = 0
        
        while True:
            page_count += 1
            params = {
                "part": "contentDetails",
                "playlistId": playlist_id,
                "maxResults": BATCH_SIZE
            }
            
            if next_page_token:
                params["pageToken"] = next_page_token
            
            data = api_client.call_api("playlistItems", params)
            
            for item in data.get("items", []):
                if video_id := item.get("contentDetails", {}).get("videoId"):
                    video_ids.append(video_id)
            
            logger.info(f"Playlist page {page_count} — collected {len(video_ids)} IDs")
            
            next_page_token = data.get("nextPageToken")
            if not next_page_token or (max_pages and page_count >= max_pages):
                break
            
            time.sleep(0.1) 
        
        return video_ids
    
    @staticmethod
    def fetch_video_details_batch(video_ids: List[str], api_client: YouTubeAPI) -> List[Dict[str, Any]]:
        """Fetch video details for a batch of IDs"""
        if not video_ids:
            return []
        
        data = api_client.call_api("videos", {
            "part": "snippet,statistics,contentDetails",
            "id": ",".join(video_ids[:BATCH_SIZE])
        })
        
        videos = []
        for item in data.get("items", []):
            snippet = item.get("snippet", {})
            stats = item.get("statistics", {})
            
            video_data = {
                "video_id": item["id"],
                "video_title": snippet.get("title", ""),
                "viewCount": VideoDataProcessor._safe_int(stats.get("viewCount")),
                "likeCount": VideoDataProcessor._safe_int(stats.get("likeCount")),
                "commentCount": VideoDataProcessor._safe_int(stats.get("commentCount")),
                "shares": 0, 
                "thumbnail_url": VideoDataProcessor._get_best_thumbnail(snippet.get("thumbnails", {})),
                "video_url": f"https://www.youtube.com/watch?v={item['id']}"
            }
            
          
            views = float(video_data["viewCount"])
            interactions = sum([
                video_data["likeCount"],
                video_data["commentCount"],
                video_data["shares"]
            ])
            
            video_data["engagement_value"] = interactions / views if views > 0 else 0.0
            videos.append(video_data)
        
        return videos
    
    @staticmethod
    def _get_best_thumbnail(thumbnails: Dict[str, Any]) -> str:
        """Get the highest quality thumbnail URL"""
        quality_order = ["maxres", "standard", "high", "medium", "default"]
        
        for quality in quality_order:
            if quality in thumbnails and thumbnails[quality].get("url"):
                return thumbnails[quality]["url"]
        
        
        for thumb in thumbnails.values():
            if isinstance(thumb, dict) and thumb.get("url"):
                return thumb["url"]
        
        return ""
    
    @staticmethod
    def _safe_int(value: Any) -> int:
        """Safely convert value to integer"""
        if value is None:
            return 0
        try:
            return int(value)
        except (ValueError, TypeError):
            return 0

class DatabaseManager:
    """Manage database operations"""
    
    def __init__(self):
        self.client = MongoClient(MONGO_URL)
        self.db = self.client['youtube_db']
        self.collection = self.db['daily_data']
    
    def ensure_tables(self):
        """Create indexes if they don't exist"""
        try:
            self.collection.create_index("video_id", unique=True)
            self.collection.create_index("channel_id")
            self.collection.create_index("retrieved_at")
        except Exception as e:
            logger.warning(f"Failed to create indexes: {e}")
    
    def insert_videos_batch(self, videos: List[Dict[str, Any]]) -> int:
        """Insert or update videos in batch"""
        if not videos:
            return 0
        
        operations = []
        for video in videos:
            video_data = video.copy()
            video_data['retrieved_at'] = datetime.now()
            
            operations.append(
                UpdateOne(
                    {"video_id": video_data["video_id"]},
                    {"$set": video_data},
                    upsert=True
                )
            )
        
        if operations:
            try:
                result = self.collection.bulk_write(operations)
                return result.upserted_count + result.modified_count
            except Exception as e:
                logger.error(f"Bulk write error: {e}")
                return 0
        return 0

def calculate_channel_aggregates(videos: List[Dict[str, Any]]) -> pd.DataFrame:
    """Calculate channel aggregates from video data"""
    if not videos:
        return pd.DataFrame()
    
    df = pd.DataFrame(videos)
    
    # Ensure numeric columns
    numeric_cols = ["viewCount", "likeCount", "commentCount", "shares", "engagement_value"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    
    # Calculate engagement count
    df["engagement_count"] = df["likeCount"] + df["commentCount"] + df["shares"]
    
    # Group by channel
    agg = df.groupby(["channel_id", "channel_handle"]).agg(
        total_number_of_videos=("video_id", "count"),
        total_views=("viewCount", "sum"),
        total_likes=("likeCount", "sum"),
        total_comments=("commentCount", "sum"),
        total_shares=("shares", "sum"),
        total_engagement=("engagement_count", "sum")
    ).reset_index()
    
    # Find top videos
    def get_top_video(df_group, metric):
        idx = df_group[metric].idxmax() if not df_group.empty else None
        if idx is not None:
            title = df_group.loc[idx, "video_title"]
            value = int(df_group.loc[idx, metric])
            return f"{title} ({value})"
        return ""
    
   
    top_videos_data = {}
    for channel_id, group in df.groupby("channel_id"):
        top_videos_data[channel_id] = {
            "likes": get_top_video(group, "likeCount"),
            "views": get_top_video(group, "viewCount"),
            "shares": get_top_video(group, "shares"),
            "thumbnail": group.nlargest(1, "viewCount")["thumbnail_url"].iloc[0] if not group.empty else ""
        }
    
 
    agg["top_video_by_likes"] = agg["channel_id"].map(lambda x: top_videos_data.get(x, {}).get("likes", ""))
    agg["top_video_by_views"] = agg["channel_id"].map(lambda x: top_videos_data.get(x, {}).get("views", ""))
    agg["top_video_by_shares"] = agg["channel_id"].map(lambda x: top_videos_data.get(x, {}).get("shares", ""))
    agg["thumbnail_url"] = agg["channel_id"].map(lambda x: top_videos_data.get(x, {}).get("thumbnail", ""))
    
   
    agg["Engagement_value"] = (
        agg["total_views"].astype(float) +
        agg["total_likes"].astype(float) +
        agg["total_comments"].astype(float) +
        agg["total_shares"].astype(float)
    ) / 4.0
    
    
    def create_thumbnail_html(url: str, width: int = 240) -> str:
        if not url or pd.isna(url):
            return ""
        url_safe = str(url).replace('"', '%22')
        return f'<img src="{url_safe}" width="{width}" />'
    
    agg["thumbnail_image_html"] = agg["thumbnail_url"].apply(create_thumbnail_html)
    
    return agg

def main():
    """Main execution function"""
    start_time = time.time()
    logger.info(f"Starting YouTube data collection for {len(CHANNEL_URLS)} channels")
    
    
    api_client = YouTubeAPI(API_KEY)
    channel_processor = YouTubeChannelProcessor(api_client)
    video_processor = VideoDataProcessor()
    db_manager = DatabaseManager()
    
   
    db_manager.ensure_tables()
    
    all_videos = []
    processed_channels = []
    
    
    for channel_url in CHANNEL_URLS:
        try:
            logger.info(f"Processing channel: {channel_url}")
            
           
            channel_id = channel_processor.resolve_channel_id(channel_url)
            if not channel_id:
                logger.error(f"Could not resolve channel ID for {channel_url}")
                continue
            
            logger.info(f"Channel ID: {channel_id}")
            
            
            playlist_id = channel_processor.get_uploads_playlist_id(channel_id)
            logger.info(f"Uploads playlist: {playlist_id}")
            
           
            video_ids = video_processor.fetch_video_ids(playlist_id, api_client)
            logger.info(f"Found {len(video_ids)} videos")
            
            if not video_ids:
                logger.warning("No videos found, skipping")
                continue
            
           
            channel_videos = []
            for i in range(0, len(video_ids), BATCH_SIZE):
                batch_ids = video_ids[i:i + BATCH_SIZE]
                batch_videos = video_processor.fetch_video_details_batch(batch_ids, api_client)
                
                
                for video in batch_videos:
                    video["channel_id"] = channel_id
                    video["channel_handle"] = channel_url.rstrip("/").split("/")[-1].replace("@", "")
                
                channel_videos.extend(batch_videos)
                logger.info(f"Fetched {len(batch_videos)} videos (batch {i//BATCH_SIZE + 1})")
                
               
                time.sleep(0.1)
            
            
            if channel_videos:
                for i in range(0, len(channel_videos), DB_BATCH_SIZE):
                    batch = channel_videos[i:i + DB_BATCH_SIZE]
                    affected = db_manager.insert_videos_batch(batch)
                    logger.info(f"Inserted/updated {len(batch)} videos to DB")
            
            all_videos.extend(channel_videos)
            processed_channels.append(channel_id)
            
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 403 and 'quotaExceeded' in e.response.text:
                logger.critical("API quota exceeded. Stopping data collection immediately.")
                break
            logger.error(f"Error processing channel {channel_url}: {e}", exc_info=True)
            continue
        except Exception as e:
            logger.error(f"Error processing channel {channel_url}: {e}", exc_info=True)
            continue
    
    if not all_videos:
        logger.error("No video data collected")
        return
    
    
    logger.info("Calculating channel aggregates...")
    aggregates = calculate_channel_aggregates(all_videos)
    
    if aggregates.empty:
        logger.error("No aggregates calculated")
        return
    
    
    if processed_channels and API_KEY:
        logger.info("Fetching additional channel info...")
        channel_info = channel_processor.get_channel_info(processed_channels)
        
        
        aggregates["channel_created_date"] = aggregates["channel_id"].map(
            lambda x: channel_info.get(x, {}).get("channel_created_date", "")
        )
        aggregates["channel_name"] = aggregates["channel_id"].map(
            lambda x: channel_info.get(x, {}).get("channel_title", "")
        )
        aggregates["total_active_subscribers"] = aggregates["channel_id"].map(
            lambda x: channel_info.get(x, {}).get("total_active_subscribers")
        )
    else:
        aggregates["channel_created_date"] = ""
        aggregates["channel_name"] = aggregates["channel_handle"]
        aggregates["total_active_subscribers"] = None
    

    output_file = f"youtube_aggregates_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    aggregates.to_csv(output_file, index=False)
    logger.info(f"Aggregates saved to {output_file}")
    
    elapsed_time = time.time() - start_time
    logger.info(f"Completed in {elapsed_time:.2f} seconds")

if __name__ == "__main__":
    main()