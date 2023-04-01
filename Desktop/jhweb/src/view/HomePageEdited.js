import React from 'react'
import { Helmet } from 'react-helmet'
import './HomePageEdited.css'

import herogirl from "../img/undrawappreciationwe8a11092-sksk.svg"
import event from "../img/undrawwaitinlineo2aq11093-et2.svg"
import img1 from "../img/vector1092-9x37.svg"
import img2 from "../img/undrawcontentteamre6rlg11091-ou7.svg"
import img3 from "../img/undrawdoctorsp6aq11099-7l8o.svg"
import img4 from "../img/undrawsignupnm4k11097-0rvs.svg"
import img5 from "../img/vector1092-9dru.svg"
import img6 from "../img/vector1092-tv6.svg"
import img7 from "../img/rectangle2491092-qpes-1600w.png"
import img8 from "../img/rectangle1941095-xwo-200h.png"
import featured_practitioner from "../img/Group 157.png"
import img10 from "../img/rectangle2121095-ezcq-200h.png"

const Hero = (props) => {
  return (
    <div className="home-page-edited-container">
      <Helmet>
        <title>Joey Helps</title>
      </Helmet>
      <div className="home-page-edited-home-page-edited">
        <span className="home-page-edited-text">
          <span>
            JoeyHelps matches licensed therapists &amp; mental health providers
            to insured Gen Z students.
          </span>
        </span>
        <span className="home-page-edited-text02">
          <span>#HealthierMindsHappierPeople</span>
        </span>
        <span className="home-page-edited-text04">
          <span>
            Affordable Therapy for better mental health.
            <span
              dangerouslySetInnerHTML={{
                __html: ' ',
              }}
            />
          </span>
        </span>
        <img
          alt="undrawappreciationwe8a11092"
          src={herogirl}
          className="home-page-edited-undrawappreciationwe8a1"
        />
        <img
          alt="Rectangle1941095"
          src={img8}
          className="home-page-edited-rectangle194"
        /> 
        <span className="home-page-edited-text06">
          <span>OCD</span>
        </span>
        <span className="home-page-edited-text08">
          <span>Relationships</span>
        </span>
        <img
          alt="Rectangle1991095"
          src={featured_practitioner}
          className="home-page-edited-rectangle199"
        />
        <span className="home-page-edited-text10">
          <span>Anxiety</span>
        </span>
        <img
          alt="Rectangle2121095"
          src={img10}
          className="home-page-edited-rectangle212"
        />
        <span className="home-page-edited-text12">
          <span>Gloria Smith</span>
        </span>
        <span className="home-page-edited-text14">
          <span>Toronto,ON</span>
        </span> 
        <img
          alt="Rectangle2161096"
          src="/playground_assets/rectangle2161096-ddt2-200h.png"
          className="home-page-edited-rectangle216"
        />
        <img
          alt="Rectangle2171096"
          src="/playground_assets/rectangle2171096-7w3e-200h.png"
          className="home-page-edited-rectangle217"
        /> 
        <div className="home-page-edited-group125">
          <div className="home-page-edited-how-our-web-works">
            <span className="home-page-edited-text16">
              <span>Personalized Match</span>
            </span>
            <span className="home-page-edited-text18">
              <span>
                Take our quiz to find personalized practitioners for your mental
                health needs
              </span>
            </span>
            <span className="home-page-edited-text20">
              <span>Curated Resources</span>
            </span>
            <span className="home-page-edited-text22">
              <span>
                Handpicked Selection of articles, podcasts, videos and resources
                tailored to helping Gen Z individuals improve their mental
                health.
                <span
                  dangerouslySetInnerHTML={{
                    __html: ' ',
                  }}
                />
              </span>
            </span>
            <span className="home-page-edited-text24">
              <span>
                <span>Mental Health</span>
                <br></br>
                <span>Practitioner List</span>
              </span>
            </span>
            <span className="home-page-edited-text29">
              <span>
                Your quiz results will prompt our practitioner list, which is
                designed for you to learn about each provider
              </span>
            </span>
            <img
              alt="undrawsignupnm4k11097"
              src={img4}
              className="home-page-edited-undrawsignupnm4k1"
            />
            <img
              alt="undrawdoctorsp6aq11099"
              src={img3}
              className="home-page-edited-undrawdoctorsp6aq1"
            />
            <img
              alt="undrawcontentteamre6rlg11091"
              src={img2}
              className="home-page-edited-undrawcontentteamre6rlg1"
            />
          </div>
          <span className="home-page-edited-text31">
            <span>How We Help</span>
          </span>
        </div>
        {/* <button className="home-page-edited-button">
          <div className="home-page-edited-outline-default">
            <span className="home-page-edited-text33">
              <span>Find your Practioner</span>
            </span>
          </div>
        </button> */}
        <div className="home-page-edited-group154">
          <img
            alt="Rectangle2491092"
            src={img7}
            className="home-page-edited-rectangle249"
          />
          <span className="home-page-edited-text35">
            <span>
              We help you learn about mental health resources to improve
              yourself
            </span>
          </span>
          <span className="home-page-edited-text37">
            <span>
              We assist you and practitioners to help make a better you
            </span>
          </span>
          <span className="home-page-edited-text39">
            <span>Education</span>
          </span>
          <span className="home-page-edited-text41">
            <span>Better You</span>
          </span>
          <span className="home-page-edited-text43">
            <span>Connection</span>
          </span>
          <span className="home-page-edited-text45">
            <span>
              We connect you to practitioners to foster personal connections for
              your mental health
              <span
                dangerouslySetInnerHTML={{
                  __html: ' ',
                }}
              />
            </span>
          </span>
          <div className="home-page-edited-frameicongraduation">
            <img
              alt="Vector1092"
              src={img6}
              className="home-page-edited-vector"
            />
          </div>
          <div className="home-page-edited-frameiconusers">
            <img
              alt="Vector1092"
              src={img5}
              className="home-page-edited-vector1"
            />
          </div>
          <div className="home-page-edited-frameiconhandholdingplant">
            <img
              alt="Vector1092"
              src={img1}
              className="home-page-edited-vector2"
            />
          </div>
          <span className="home-page-edited-text47">
            <span>Supporting your journey...</span>
          </span>
        </div>

        {/* <div className="home-page-edited-nav-bar">
          <img
            alt="Rectangle54I109"
            src="/playground_assets/rectangle54i109-x7ao-200h.png"
            className="home-page-edited-rectangle54"
          />
          <span className="home-page-edited-text49">
            <span>About Us</span>
          </span>
          <span className="home-page-edited-text51">
            <span>Event</span>
          </span>
          <span className="home-page-edited-text53">
            <span>Resources</span>
          </span>
          <button className="home-page-edited-button-primary">
            <span className="home-page-edited-text55">
              <span>Sign Up</span>
            </span>
          </button>
          <span className="home-page-edited-text57">
            <span>Home</span>
          </span>
          <div className="home-page-edited-group91">
            <span className="home-page-edited-text59">
              <span>JoeyHelps</span>
            </span>
          </div>
          <img
            alt="Rectangle55I109"
            src="/playground_assets/rectangle55i109-ae8m-200h.png"
            className="home-page-edited-rectangle55"
          />
          <span className="home-page-edited-text61">
            <span>About Us</span>
          </span>
          <span className="home-page-edited-text63">
            <span>Event</span>
          </span>
          <span className="home-page-edited-text65">
            <span>Find a practioner</span>
          </span>
          <span className="home-page-edited-text67">
            <span>Resources</span>
          </span>
          <span className="home-page-edited-text69">
            <span>Home</span>
          </span>
          <div className="home-page-edited-group100">
            <img
              alt="JoeyHelps21I109"
              src="/playground_assets/joeyhelps21i109-7wla-200h.png"
              className="home-page-edited-joey-helps21"
            />
            <div className="home-page-edited-group92">
              <span className="home-page-edited-text71">
                <span>JoeyHelps</span>
              </span>
            </div>
          </div>
          <span className="home-page-edited-text73">
            <span>For practitioner</span>
          </span>
        </div> */}
        <div className="home-page-edited-group155">
          <div className="home-page-edited-group118">
            <div className="home-page-edited-group99">
              <img
                alt="undrawwaitinlineo2aq11093"
                src={event}
                className="home-page-edited-undrawwaitinlineo2aq1"
              />
              <span className="home-page-edited-text75">
                <span>FIRST STEP SUMMIT</span>
              </span>
              <span className="home-page-edited-text77">
                <span>Mental Health Conference</span>
              </span>
              <span className="home-page-edited-text79">
                <span>April 6, 2023</span>
              </span>
            </div>
          </div>
          <div className="home-page-edited-group153">
          <a className ="home-page-edited-text81" href='https://www.eventbrite.com/e/joey-helps-x-deloitte-first-step-summit-tickets-583867271727' target="_blank">View Details</a>
            <span className="home-page-edited-text81"> 
            </span>
          </div>
        </div>
      </div>
    </div>
  )
}

export default Hero 
