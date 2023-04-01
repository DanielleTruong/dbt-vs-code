import { Header } from "../view/Header";
import { Footer } from "../view/Footer";
import "./Practitioner.css";
import practitioner_page from "../img/practitioner_page.png";
import Focus from "../img/Focus.png";
import Content from "../img/Content.png";
import Connection from "../img/Connection.png";
import Interview from "../img/Interview.png";
import New_Practices from "../img/New_Practices.png";

export function Practitioner() {
  return (
    <div>
      <Header />
      <div className="practitioner_first_box">
        <div className="description">
          <div className="description_title">
            Helping you find long-term clients.
          </div>
          <br />
          <br />
          <br />
          <button className="description_pricing_plan_button">
            View Pricing Plans
          </button>
        </div>
        <div className="first_box_image">
          <img
            src={practitioner_page}
            style={{ float: "right", height: 550 }}></img>
        </div>
      </div>

			<div className="practitioner_second_box">
				<div className="second_box_title">
					Why use Joey Helps?
				</div>
				<br/>
				<br/>
				<br/>
				<div className="second_box_description">
				JoeyHelps Gen Z focused audience and branding sets us apart from the competition,
				we are  helping you build a long-term client-base and empower you with the tools 
				to connect with Gen Z consumers. 
				</div>
				<div className="boxes_top">
					<div className="Focus">
						<img className = "Focus_img" src = {Focus} style={{ float: "left"}} ></img>
						<div className="Focus_box">
							<div className="Focus_title">Gen Z Focus</div>
							<div className="Focus_description">Focused on helping you build a client base with young adults 18-25.</div>
						</div>
					</div>


					<div className="Content">
						<img className = "Content_img" src = {Content} style={{ float: "left"}} ></img>
							<div className="Content_box">
								<div className="Content_title">Custom Content</div>
								<div className="Content_description">We create social media posts and advertise your practice on the platformed most used by Gen Z. </div>
							</div>
					</div>


				</div>
				<div className="boxes_bottom">
					<div className="Connection">
						<img className = "Connection_img" src = {Connection} style={{ float: "left"}} ></img>
							<div className="Connection_box">
							<div className="Connection_title">Build Connection</div>
							<div className="Connection_description">Using custom descriptions and content, we help you build long-term connection with your new clientele. </div>
							</div>
					</div>
					
					<div className="Interview">
						<img className = "Interview_img" src = {Interview} style={{ float: "left"}} ></img>
								<div className="Interview_box">
								<div className="Interview_title">Interviews</div>
								<div className="Interview_description">We will create a 5-10 minute interview video, where you can describe your practice and attract new clients. </div>
								</div>
					</div>
				</div>
			</div>
			<br/>
			<br/>
			<br/>
			<br/>
			<br/>
			<br/>
			<div className="practitioner_third_box">
				<div className="New_Practices">
					<img className = "New_Practices_img" src = {New_Practices} style={{ float: "left" , height: 300}} ></img>
					<div className="New_Practices_box">
						<div className="New_Practices_title">Helping New Practices</div>
						<div className="New_Practices_description">
							<ul className="a">
								<li>We are focused on helping new practices and practitioners grow their client base!</li>
								<li>Need help marketing? New grad? We’re here to help.</li>
								<li>Strong understanding of Gen Z market.</li>
								<li>Show your story and start building trust.</li>
							</ul>
						</div>
					</div>
				</div>
			</div>

      <Footer />
    </div>
  );
}
