import { connect } from "react-redux";
import { State } from "metabase-types/store";
import SlackSetup from "../../components/SlackSetup";
import SlackSetupForm from "../../components/SlackSetupForm";
import { updateSettings } from "../../actions";
import { hasSlackBotToken } from "../../selectors";

const mapStateToProps = (state: State) => ({
  Form: SlackSetupForm,
  hasBot: hasSlackBotToken(state),
  hasError: false,
});

const mapDispatchToProps = {
  onSubmit: updateSettings,
};

export default connect(mapStateToProps, mapDispatchToProps)(SlackSetup);
