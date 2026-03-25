package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/esadakcam/conductor/internal/logger"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

// Handler holds dependencies for HTTP handlers
type Handler struct {
	k8sClient        KubernetesClient
	epochValidator   Validator
	idempotencyGuard IdempotencyGuard
}

// NewHandler creates a new Handler
func NewHandler(k8sClient KubernetesClient, epochValidator Validator, idempotencyGuard IdempotencyGuard) *Handler {
	return &Handler{
		k8sClient:        k8sClient,
		epochValidator:   epochValidator,
		idempotencyGuard: idempotencyGuard,
	}
}

// Get handles GET /api/v1/{resource}/{namespace}/{name}
func (h *Handler) HandleGet(w http.ResponseWriter, r *http.Request) {
	resource := r.PathValue("resource")
	namespace := r.PathValue("namespace")
	name := r.PathValue("name")

	obj, err := h.k8sClient.Get(r.Context(), resource, namespace, name)
	if err != nil {
		logger.Errorf("Failed to get resource %s/%s/%s: %v", resource, namespace, name, err)
		h.sendError(w, http.StatusInternalServerError, err.Error())
		return
	}
	logger.Infof("Successfully retrieved resource %s/%s/%s", resource, namespace, name)
	h.sendJSON(w, http.StatusOK, obj)
}

// List handles GET /api/v1/{resource}/{namespace} and /api/v1/{resource}
func (h *Handler) HandleList(w http.ResponseWriter, r *http.Request) {
	resource := r.PathValue("resource")
	namespace := r.PathValue("namespace") // Can be empty if pattern is /api/v1/{resource}

	list, err := h.k8sClient.List(r.Context(), resource, namespace)
	if err != nil {
		logger.Errorf("Failed to list resource %s in namespace %s: %v", resource, namespace, err)
		h.sendError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if namespace == "" {
		logger.Infof("Successfully listed all %s resources", resource)
	} else {
		logger.Infof("Successfully listed %s resources in namespace %s", resource, namespace)
	}
	h.sendJSON(w, http.StatusOK, list)
}

// Create handles POST /api/v1/{resource}/{namespace}
func (h *Handler) HandleCreate(w http.ResponseWriter, r *http.Request) {
	idempotencyId, ok := h.reserveIdempotencyKey(w, r)
	if !ok {
		return
	}

	resource := r.PathValue("resource")
	namespace := r.PathValue("namespace")

	var req CreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.Warnf("Invalid request body for create %s/%s: %v", resource, namespace, err)
		h.releaseIdempotencyKey(r.Context(), idempotencyId)
		h.sendError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if !h.validateEpoch(w, r.Context(), req.Epoch, fmt.Sprintf("create %s/%s", resource, namespace)) {
		h.releaseIdempotencyKey(r.Context(), idempotencyId)
		return
	}

	objMap, ok := req.Object.(map[string]interface{})
	if !ok {
		logger.Warnf("Invalid object format for create %s/%s", resource, namespace)
		h.releaseIdempotencyKey(r.Context(), idempotencyId)
		h.sendError(w, http.StatusBadRequest, "invalid object format")
		return
	}

	u := &unstructured.Unstructured{Object: objMap}
	created, err := h.k8sClient.Create(r.Context(), resource, namespace, u)
	if err != nil {
		logger.Errorf("Failed to create resource %s/%s: %v", resource, namespace, err)
		h.releaseIdempotencyKey(r.Context(), idempotencyId)
		h.sendError(w, http.StatusInternalServerError, err.Error())
		return
	}
	createdName := created.GetName()
	logger.Infof("Successfully created resource %s/%s/%s (epoch: %d)", resource, namespace, createdName, req.Epoch)
	h.sendJSON(w, http.StatusCreated, created)
}

// Update handles PUT /api/v1/{resource}/{namespace}/{name}
func (h *Handler) HandleUpdate(w http.ResponseWriter, r *http.Request) {
	idempotencyId, ok := h.reserveIdempotencyKey(w, r)
	if !ok {
		return
	}

	resource := r.PathValue("resource")
	namespace := r.PathValue("namespace")
	name := r.PathValue("name")

	var req UpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.Warnf("Invalid request body for update %s/%s/%s: %v", resource, namespace, name, err)
		h.releaseIdempotencyKey(r.Context(), idempotencyId)
		h.sendError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if !h.validateEpoch(w, r.Context(), req.Epoch, fmt.Sprintf("update %s/%s/%s", resource, namespace, name)) {
		h.releaseIdempotencyKey(r.Context(), idempotencyId)
		return
	}

	objMap, ok := req.Object.(map[string]interface{})
	if !ok {
		logger.Warnf("Invalid object format for update %s/%s/%s", resource, namespace, name)
		h.releaseIdempotencyKey(r.Context(), idempotencyId)
		h.sendError(w, http.StatusBadRequest, "invalid object format")
		return
	}

	u := &unstructured.Unstructured{Object: objMap}
	if u.GetName() != name {
		u.SetName(name)
	}

	updated, err := h.k8sClient.Update(r.Context(), resource, namespace, u)
	if err != nil {
		logger.Errorf("Failed to update resource %s/%s/%s: %v", resource, namespace, name, err)
		h.releaseIdempotencyKey(r.Context(), idempotencyId)
		h.sendError(w, http.StatusInternalServerError, err.Error())
		return
	}
	logger.Infof("Successfully updated resource %s/%s/%s (epoch: %d)", resource, namespace, name, req.Epoch)
	h.sendJSON(w, http.StatusOK, updated)
}

// Patch handles PATCH /api/v1/{resource}/{namespace}/{name}
func (h *Handler) HandlePatch(w http.ResponseWriter, r *http.Request) {
	idempotencyId, reserved := h.reserveIdempotencyKey(w, r)
	if !reserved {
		return
	}

	resource := r.PathValue("resource")
	namespace := r.PathValue("namespace")
	name := r.PathValue("name")

	var bodyMap map[string]interface{}
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		logger.Warnf("Failed to read body for patch %s/%s/%s: %v", resource, namespace, name, err)
		h.releaseIdempotencyKey(r.Context(), idempotencyId)
		h.sendError(w, http.StatusBadRequest, "failed to read body")
		return
	}

	if err := json.Unmarshal(bodyBytes, &bodyMap); err != nil {
		logger.Warnf("Invalid JSON for patch %s/%s/%s: %v", resource, namespace, name, err)
		h.releaseIdempotencyKey(r.Context(), idempotencyId)
		h.sendError(w, http.StatusBadRequest, "invalid json")
		return
	}

	epochVal, ok := bodyMap["epoch"].(float64)
	if !ok {
		logger.Warnf("Missing or invalid epoch for patch %s/%s/%s", resource, namespace, name)
		h.releaseIdempotencyKey(r.Context(), idempotencyId)
		h.sendError(w, http.StatusBadRequest, "epoch is required and must be a number")
		return
	}
	epoch := int64(epochVal)

	if !h.validateEpoch(w, r.Context(), epoch, fmt.Sprintf("patch %s/%s/%s", resource, namespace, name)) {
		h.releaseIdempotencyKey(r.Context(), idempotencyId)
		return
	}

	patchData, ok := bodyMap["patch"]
	if !ok {
		logger.Warnf("Missing patch data for patch %s/%s/%s", resource, namespace, name)
		h.releaseIdempotencyKey(r.Context(), idempotencyId)
		h.sendError(w, http.StatusBadRequest, "patch data is required")
		return
	}

	patchBytes, err := json.Marshal(patchData)
	if err != nil {
		logger.Errorf("Failed to marshal patch data for %s/%s/%s: %v", resource, namespace, name, err)
		h.releaseIdempotencyKey(r.Context(), idempotencyId)
		h.sendError(w, http.StatusInternalServerError, "failed to marshal patch data")
		return
	}

	patched, err := h.k8sClient.Patch(r.Context(), resource, namespace, name, types.MergePatchType, patchBytes)
	if err != nil {
		logger.Errorf("Failed to patch resource %s/%s/%s: %v", resource, namespace, name, err)
		h.releaseIdempotencyKey(r.Context(), idempotencyId)
		h.sendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	logger.Infof("Successfully patched resource %s/%s/%s (epoch: %d)", resource, namespace, name, epoch)
	h.sendJSON(w, http.StatusOK, patched)
}

// Delete handles DELETE /api/v1/{resource}/{namespace}/{name}
func (h *Handler) HandleDelete(w http.ResponseWriter, r *http.Request) {
	idempotencyId, ok := h.reserveIdempotencyKey(w, r)
	if !ok {
		return
	}

	resource := r.PathValue("resource")
	namespace := r.PathValue("namespace")
	name := r.PathValue("name")

	var req DeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.Warnf("Invalid request body for delete %s/%s/%s: %v", resource, namespace, name, err)
		h.releaseIdempotencyKey(r.Context(), idempotencyId)
		h.sendError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if !h.validateEpoch(w, r.Context(), req.Epoch, fmt.Sprintf("delete %s/%s/%s", resource, namespace, name)) {
		h.releaseIdempotencyKey(r.Context(), idempotencyId)
		return
	}

	if err := h.k8sClient.Delete(r.Context(), resource, namespace, name); err != nil {
		logger.Errorf("Failed to delete resource %s/%s/%s: %v", resource, namespace, name, err)
		h.releaseIdempotencyKey(r.Context(), idempotencyId)
		h.sendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	logger.Infof("Successfully deleted resource %s/%s/%s (epoch: %d)", resource, namespace, name, req.Epoch)
	w.WriteHeader(http.StatusNoContent)
}

// HandleExecDeployment handles POST /api/v1/exec/deployments/{namespace}/{name}
// Executes a command on all running pods of a deployment
func (h *Handler) HandleExecDeployment(w http.ResponseWriter, r *http.Request) {
	idempotencyId, ok := h.reserveIdempotencyKey(w, r)
	if !ok {
		return
	}

	namespace := r.PathValue("namespace")
	name := r.PathValue("name")

	var req ExecDeploymentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.Warnf("Invalid request body for exec deployment %s/%s: %v", namespace, name, err)
		h.releaseIdempotencyKey(r.Context(), idempotencyId)
		h.sendError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if !h.validateEpoch(w, r.Context(), req.Epoch, fmt.Sprintf("exec deployment %s/%s", namespace, name)) {
		h.releaseIdempotencyKey(r.Context(), idempotencyId)
		return
	}

	if len(req.Command) == 0 {
		logger.Warnf("Missing command for exec deployment %s/%s", namespace, name)
		h.releaseIdempotencyKey(r.Context(), idempotencyId)
		h.sendError(w, http.StatusBadRequest, "command is required")
		return
	}

	results, err := h.k8sClient.ExecDeployment(r.Context(), namespace, name, req.Container, req.Command)
	if err != nil {
		logger.Errorf("Failed to exec on deployment %s/%s: %v", namespace, name, err)
		h.releaseIdempotencyKey(r.Context(), idempotencyId)
		h.sendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Convert k8s.PodExecResult to JSON-serializable format
	jsonResults := make([]PodExecResultJSON, len(results))
	for i, r := range results {
		jsonResults[i] = PodExecResultJSON{
			PodName: r.PodName,
		}
		if r.Result != nil {
			jsonResults[i].Result = &ExecResultJSON{
				Stdout: r.Result.Stdout,
				Stderr: r.Result.Stderr,
			}
		}
		if r.Error != nil {
			jsonResults[i].Error = r.Error.Error()
		}
	}

	response := ExecDeploymentResponse{
		DeploymentName: name,
		Namespace:      namespace,
		Results:        jsonResults,
	}

	logger.Infof("Successfully executed command on deployment %s/%s (epoch: %d, pods: %d)", namespace, name, req.Epoch, len(results))
	h.sendJSON(w, http.StatusOK, response)
}

// HandleWaitDeploymentRollout handles POST /api/v1/rollout/deployments/{namespace}/{name}
// Waits for a deployment rollout to complete
func (h *Handler) HandleWaitDeploymentRollout(w http.ResponseWriter, r *http.Request) {
	idempotencyId, ok := h.reserveIdempotencyKey(w, r)
	if !ok {
		return
	}

	namespace := r.PathValue("namespace")
	name := r.PathValue("name")

	var req WaitDeploymentRolloutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.Warnf("Invalid request body for wait rollout deployment %s/%s: %v", namespace, name, err)
		h.releaseIdempotencyKey(r.Context(), idempotencyId)
		h.sendError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if !h.validateEpoch(w, r.Context(), req.Epoch, fmt.Sprintf("wait rollout deployment %s/%s", namespace, name)) {
		h.releaseIdempotencyKey(r.Context(), idempotencyId)
		return
	}

	timeout := 5 * time.Minute
	if req.Timeout != "" {
		parsed, err := time.ParseDuration(req.Timeout)
		if err != nil {
			logger.Warnf("Invalid timeout format for wait rollout deployment %s/%s: %v", namespace, name, err)
			h.releaseIdempotencyKey(r.Context(), idempotencyId)
			h.sendError(w, http.StatusBadRequest, "invalid timeout format")
			return
		}
		timeout = parsed
	}

	err := h.k8sClient.WaitForDeploymentRollout(r.Context(), namespace, name, timeout)
	if err != nil {
		logger.Errorf("Failed to wait for rollout of deployment %s/%s: %v", namespace, name, err)
		h.releaseIdempotencyKey(r.Context(), idempotencyId)
		h.sendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	response := WaitDeploymentRolloutResponse{
		DeploymentName: name,
		Namespace:      namespace,
		Status:         "completed",
	}

	logger.Infof("Successfully waited for deployment rollout %s/%s (epoch: %d)", namespace, name, req.Epoch)
	h.sendJSON(w, http.StatusOK, response)
}

func (h *Handler) sendError(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(ErrorResponse{
		Error: message,
		Code:  statusCode,
	})
}

func (h *Handler) sendJSON(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(data)
}

// reserveIdempotencyKey extracts the X-Idempotency-Id header and
// atomically reserves it via the IdempotencyGuard.
func (h *Handler) reserveIdempotencyKey(w http.ResponseWriter, r *http.Request) (string, bool) {
	idempotencyId := r.Header.Get("X-Idempotency-Id")
	if idempotencyId == "" {
		logger.Warnf("Missing idempotency id for create")
		h.sendError(w, http.StatusBadRequest, "idempotency id is required")
		return "", false
	}
	reserved, err := h.idempotencyGuard.Reserve(r.Context(), idempotencyId)
	if err != nil {
		logger.Errorf("Idempotency reservation failed: %v", err)
		h.sendError(w, http.StatusInternalServerError, "idempotency validation failed")
		return "", false
	}
	if !reserved {
		logger.Warnf("Idempotency %s already processed", idempotencyId)
		h.sendJSON(w, http.StatusNoContent, "idempotency already processed")
		return "", false
	}
	return idempotencyId, true
}

func (h *Handler) releaseIdempotencyKey(ctx context.Context, idempotencyId string) {
	if idempotencyId == "" {
		return
	}
	if err := h.idempotencyGuard.Release(ctx, idempotencyId); err != nil {
		logger.Errorf("Failed to release idempotency key %s: %v", idempotencyId, err)
	}
}

func (h *Handler) validateEpoch(w http.ResponseWriter, ctx context.Context, epoch int64, operationDesc string) bool {
	valid, err := h.epochValidator.Validate(ctx, epoch)
	if err != nil {
		logger.Errorf("Epoch validation failed for %s: %v", operationDesc, err)
		h.sendError(w, http.StatusInternalServerError, fmt.Sprintf("epoch validation failed: %v", err))
		return false
	}
	if !valid {
		logger.Warnf("Stale epoch %d for %s", epoch, operationDesc)
		h.sendError(w, http.StatusConflict, "stale epoch")
		return false
	}
	return true
}
